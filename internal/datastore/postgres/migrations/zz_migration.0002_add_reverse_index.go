package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`
	createReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation)`
)

func init() {
	if err := DatabaseMigrations.Register("add-reverse-index", "1eaeba4b8a73", func(ctx context.Context, conn *pgx.Conn, version, replaced string) error {
		return commitWithMigrationVersion(ctx, conn, version, replaced, func(tx pgx.Tx) error {
			for _, stmt := range []string{
				createReverseQueryIndex,
				createReverseCheckIndex,
			} {
				_, err := tx.Exec(ctx, stmt)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
