package ch

import (
	"context"
	"fmt"
	"github.com/pingcap/go-tpc/pkg/util"
)

var allTables []string

func init() {
	allTables = []string{"customer", "district", "history", "item", "new_order", "order_line", "orders", "region", "warehouse",
		"nation", "stock", "supplier"}
}

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableName string, action string) error {
	s := w.getState(ctx)
	fmt.Printf("%s %s\n", action, tableName)
	if ctx.Value("risingwave") != nil && ctx.Value("risingwave").(bool) {
		query = util.ConvertToRisingWaveDDL(query)
	}
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *Workloader) createTables(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS nation (
    N_NATIONKEY BIGINT NOT NULL,
    N_NAME VARCHAR(25) NOT NULL,
    N_REGIONKEY BIGINT NOT NULL,
    N_COMMENT VARCHAR(152),
    PRIMARY KEY (N_NATIONKEY)
)`

	if err := w.createTableDDL(ctx, query, "nation", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS region (
    R_REGIONKEY BIGINT NOT NULL,
    R_NAME VARCHAR(25) NOT NULL,
    R_COMMENT VARCHAR(152),
    PRIMARY KEY (R_REGIONKEY)
)`
	if err := w.createTableDDL(ctx, query, "region", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS supplier (
    S_SUPPKEY BIGINT NOT NULL,
    S_NAME VARCHAR(25) NOT NULL,
    S_ADDRESS VARCHAR(40) NOT NULL,
    S_NATIONKEY BIGINT NOT NULL,
    S_PHONE VARCHAR(15) NOT NULL,
    S_ACCTBAL DECIMAL(15, 2) NOT NULL,
    S_COMMENT VARCHAR(101) NOT NULL,
    PRIMARY KEY (S_SUPPKEY)
)`
	if err := w.createTableDDL(ctx, query, "supplier", "creating"); err != nil {
		return err
	}

	return nil
}
