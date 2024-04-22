package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	outputPlugin   = "pgoutput"
	standbyTimeout = time.Second * 10
)

var typeMap = pgtype.NewMap()

type PGConsumer struct {
	conn            *pgconn.PgConn
	adminConn       *pgconn.PgConn
	tableName       string
	publicationName string
	relations       map[uint32]*pglogrepl.RelationMessageV2
	clientPos       atomic.Uint64
	inStream        atomic.Bool
	wg              sync.WaitGroup
}

func NewPGConsumer(ctx context.Context, db string) (*PGConsumer, error) {
	conn, err := pgconn.Connect(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("unable to connect: %w", err)
	}
	adminConn, err := pgconn.Connect(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("unable to connect: %w", err)
	}
	err = conn.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return &PGConsumer{
		conn:      conn,
		adminConn: adminConn,
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		clientPos: atomic.Uint64{},
		inStream:  atomic.Bool{},
	}, nil
}

func (pgc *PGConsumer) Start(ctx context.Context, tableName string) error {
	pgc.tableName = tableName
	publicationName := fmt.Sprintf("pgtap_%s", tableName)
	pgc.publicationName = publicationName
	err := pgc.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+publicationName).Close()
	if err != nil {
		return fmt.Errorf("unable to drop publication %q: %w", publicationName, err)
	}
	createPub := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publicationName, tableName)
	err = pgc.conn.Exec(ctx, createPub).Close()
	if err != nil {
		return fmt.Errorf("failed to create publication %s: %w", publicationName, err)
	}
	sysident, err := pglogrepl.IdentifySystem(ctx, pgc.conn)
	if err != nil {
		return fmt.Errorf("unable to identify system: %w", err)
	}
	pgc.clientPos.Store(uint64(sysident.XLogPos))
	slog.Info("pg identified",
		"system_id", sysident.SystemID,
		"timeline", sysident.Timeline,
		"xlog_pos", sysident.XLogPos,
		"db_name", sysident.DBName,
	)
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publicationName),
		"messages 'true'",
		"streaming 'true'",
	}
	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		pgc.conn,
		publicationName,
		outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: true,
		})
	if err != nil {
		return fmt.Errorf("CreateReplicationSlot failed: %w", err)
	}
	err = pglogrepl.StartReplication(
		ctx,
		pgc.conn,
		publicationName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs},
	)
	if err != nil {
		return fmt.Errorf("StartReplication failed: %w", err)
	}
	err = pgc.Consume(ctx)

	return err
}

func (pgc *PGConsumer) Consume(ctx context.Context) error {
	nextStandbyMessageDeadline := time.Now().Add(standbyTimeout)
	pgc.wg.Add(1)
	go func() {
		defer pgc.wg.Done()
		slog.Info("starting consumer")
		for {
			baseCtx := ctx
			if baseCtx.Err() != nil {
				return
			}
			if time.Now().After(nextStandbyMessageDeadline) {
				pos := pglogrepl.LSN(pgc.clientPos.Load())
				err := pglogrepl.SendStandbyStatusUpdate(ctx, pgc.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
				if err != nil {
					slog.Error("SendStandbyStatusUpdate failed", "error", err)
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
			rawMsg, err := pgc.conn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				slog.Error("ReceiveMessage failed", "error", err)
				return
			}
			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				slog.Error("received WAL error", "error", errMsg)
				return
			}
			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				slog.Warn("Received unexpected message", "message", msg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				if err = pgc.HandleKeepAlive(msg.Data[1:], &nextStandbyMessageDeadline); err != nil {
					slog.Error("HandleKeepAlive error", "error", err)
				}
			case pglogrepl.XLogDataByteID:
				if err = pgc.HandleXLogData(msg.Data[1:]); err != nil {
					slog.Error("HandleXLogData", "error", err)
				}
			default:
				slog.Error("unknown byte", "byte", msg.Data[0])
			}
		}
	}()
	return nil
}

func (pgc *PGConsumer) HandleKeepAlive(data []byte, deadline *time.Time) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
	}
	slog.Debug("Primary Keepalive Message",
		"server_wal_end:", pkm.ServerWALEnd,
		"server_time", pkm.ServerTime,
		"reply_requested", pkm.ReplyRequested,
	)
	if pkm.ServerWALEnd > pglogrepl.LSN(pgc.clientPos.Load()) {
		pgc.clientPos.Store(uint64(pkm.ServerWALEnd))
	}
	if pkm.ReplyRequested {
		deadline = &time.Time{}
	}
	return nil
}

func (pgc *PGConsumer) HandleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("ParseXLogData failed: %w", err)
	}

	slog.Info("XLogData",
		"wal_start", xld.WALStart,
		"wal_end", xld.ServerWALEnd,
		"server_time", xld.ServerTime,
	)
	err = pgc.HandleV2(xld.WALData)
	if err != nil {
		slog.Error("HandleV2", "error", err)
	}

	if xld.WALStart > pglogrepl.LSN(pgc.clientPos.Load()) {
		pgc.clientPos.Store(uint64(xld.WALStart))
	}
	return nil
}

func (pgc *PGConsumer) Close() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()
	slog.Info("closing consumer")
	pgc.wg.Wait()
	pgc.conn.Close(ctx)
	err := pgc.adminConn.Exec(ctx, "DROP PUBLICATION "+pgc.publicationName).Close()
	if err != nil {
		slog.Error("failed to drop publication", "error", err)
	}
	pgc.adminConn.Close(ctx)
	slog.Info("consumer closed")
}

func (pgc *PGConsumer) HandleV2(walData []byte) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, pgc.inStream.Load())
	if err != nil {
		return fmt.Errorf("Parse logical replication message: %w", err)
	}
	slog.Info("Receive a logical replication message", "type", logicalMsg.Type())
	switch logicalMsg := logicalMsg.(type) {

	case *pglogrepl.RelationMessageV2:
		pgc.relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction.
		// This is only sent for committed transactions. You won't get
		// any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, values, err := pgc.decodeColumns(logicalMsg.RelationID, logicalMsg.Tuple.Columns)
		if err != nil {
			return fmt.Errorf("failed to decode columns: %w", err)
		}
		slog.Info("INSERT",
			"action", "insert",
			"table", rel.RelationName,
			"values", values)
	case *pglogrepl.UpdateMessageV2:
		rel, newValues, err := pgc.decodeColumns(logicalMsg.RelationID, logicalMsg.NewTuple.Columns)
		if err != nil {
			return fmt.Errorf("failed to decode columns: %w", err)
		}
		_, oldValues, err := pgc.decodeColumns(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		slog.Info("update", "xid", logicalMsg.Xid)
		slog.Info("UPDATE",
			"action", "update",
			"table", rel.RelationName,
			"deltas", findDelta(oldValues, newValues))
	case *pglogrepl.DeleteMessageV2:
		_, values, err := pgc.decodeColumns(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		if err != nil {
			return fmt.Errorf("failed to decode columns: %w", err)
		}
		slog.Info("delete",
			"xid", logicalMsg.Xid,
			"columns", logicalMsg.OldTuple.Columns,
			"values", values,
			"action", "delete",
		)
	case *pglogrepl.TruncateMessageV2:
		slog.Info("truncate", "xid", logicalMsg.Xid)
	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:
	case *pglogrepl.LogicalDecodingMessageV2:
		slog.Info("Logical decoding message",
			"prefix", logicalMsg.Prefix,
			"content", logicalMsg.Content,
			"xid", logicalMsg.Xid,
		)
	case *pglogrepl.StreamStartMessageV2:
		pgc.inStream.Store(true)
		slog.Info("Stream start message",
			"xid", logicalMsg.Xid,
			"first_segment", logicalMsg.FirstSegment,
		)
	case *pglogrepl.StreamStopMessageV2:
		pgc.inStream.Store(false)
		slog.Info("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
	case *pglogrepl.StreamAbortMessageV2:
	default:
		return fmt.Errorf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
	return nil
}

func (pgc *PGConsumer) decodeColumns(
	relID uint32,
	columns []*pglogrepl.TupleDataColumn,
) (*pglogrepl.RelationMessageV2, map[string]any, error) {
	rel, ok := pgc.relations[relID]
	if !ok {
		return nil, nil, fmt.Errorf("unknown relation ID %d", relID)
	}
	values := make(map[string]interface{}, len(columns))
	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple,
			// and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return rel, nil, fmt.Errorf("error decoding column data: %w", err)
			}
			values[colName] = val
		}
	}
	return rel, values, nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func findDelta(oldMap, newMap map[string]any) []string {
	deltas := []string{}
	for oldKey, oldVal := range oldMap {
		if newVal, ok := newMap[oldKey]; !ok || oldVal != newVal {
			deltas = append(deltas, oldKey)
		}
	}
	for newKey := range newMap {
		if _, ok := oldMap[newKey]; !ok {
			deltas = append(deltas, newKey)
		}
	}
	return deltas
}
