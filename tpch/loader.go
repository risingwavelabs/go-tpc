package tpch

import (
	"context"
	"database/sql"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/pingcap/go-tpc/pkg/sink"
	"github.com/pingcap/go-tpc/tpch/dbgen"
)

type sqlLoader struct {
	*sink.ConcurrentSink
	context.Context
}

func (s *sqlLoader) WriteRow(values ...interface{}) error {
	return s.ConcurrentSink.WriteRow(s.Context, values...)
}

func (s *sqlLoader) Flush() error {
	return s.ConcurrentSink.Flush(s.Context)
}

type orderLoader struct {
	sqlLoader
}

func (o *orderLoader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	return o.WriteRow(
		order.OKey,
		order.CustKey,
		order.Status,
		dbgen.FmtMoney(order.TotalPrice),
		order.Date,
		order.OrderPriority,
		order.Clerk,
		order.ShipPriority,
		order.Comment,
	)
}

type custLoader struct {
	sqlLoader
}

func (c *custLoader) Load(item interface{}) error {
	cust := item.(*dbgen.Cust)
	return c.WriteRow(
		cust.CustKey,
		cust.Name,
		cust.Address,
		cust.NationCode,
		cust.Phone,
		dbgen.FmtMoney(cust.Acctbal),
		cust.MktSegment,
		cust.Comment,
	)
}

type lineItemloader struct {
	sqlLoader
}

func (l *lineItemloader) Load(item interface{}) error {
	order := item.(*dbgen.Order)
	for _, line := range order.Lines {
		if err := l.WriteRow(
			line.OKey,
			line.PartKey,
			line.SuppKey,
			line.LCnt,
			line.Quantity,
			dbgen.FmtMoney(line.EPrice),
			dbgen.FmtMoney(line.Discount),
			dbgen.FmtMoney(line.Tax),
			line.RFlag,
			line.LStatus,
			line.SDate,
			line.CDate,
			line.RDate,
			line.ShipInstruct,
			line.ShipMode,
			line.Comment,
		); err != nil {
			return nil
		}
	}
	return nil
}

type nationLoader struct {
	sqlLoader
}

func (n *nationLoader) Load(item interface{}) error {
	nation := item.(*dbgen.Nation)
	return n.WriteRow(
		nation.Code,
		nation.Text,
		nation.Join,
		nation.Comment,
	)
}

type partLoader struct {
	sqlLoader
}

func (p *partLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	return p.WriteRow(
		part.PartKey,
		part.Name,
		part.Mfgr,
		part.Brand,
		part.Type,
		part.Size,
		part.Container,
		dbgen.FmtMoney(part.RetailPrice),
		part.Comment,
	)
}

type partSuppLoader struct {
	sqlLoader
}

func (p *partSuppLoader) Load(item interface{}) error {
	part := item.(*dbgen.Part)
	for _, supp := range part.S {
		if err := p.WriteRow(
			supp.PartKey,
			supp.SuppKey,
			supp.Qty,
			dbgen.FmtMoney(supp.SCost),
			supp.Comment,
		); err != nil {
			return err
		}
	}
	return nil
}

type suppLoader struct {
	sqlLoader
}

func (s *suppLoader) Load(item interface{}) error {
	supp := item.(*dbgen.Supp)
	return s.WriteRow(
		supp.SuppKey,
		supp.Name,
		supp.Address,
		supp.NationCode,
		supp.Phone,
		dbgen.FmtMoney(supp.Acctbal),
		supp.Comment,
	)
}

type regionLoader struct {
	sqlLoader
}

func (r *regionLoader) Load(item interface{}) error {
	region := item.(*dbgen.Region)
	return r.WriteRow(
		region.Code,
		region.Text,
		region.Comment,
	)
}

func NewOrderLoader(ctx context.Context, db *sql.DB, concurrency int) *orderLoader {
	return &orderLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO orders (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaOrderLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *orderLoader {
	cols := []string{"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"}
	return &orderLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("orders", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewLineItemLoader(ctx context.Context, db *sql.DB, concurrency int) *lineItemloader {
	return &lineItemloader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO lineitem (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaLineItemLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *lineItemloader {
	cols := []string{"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"}
	return &lineItemloader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("lineitem", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewCustLoader(ctx context.Context, db *sql.DB, concurrency int) *custLoader {
	return &custLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO customer (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaCustLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *custLoader {
	cols := []string{"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"}
	return &custLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("customer", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewPartLoader(ctx context.Context, db *sql.DB, concurrency int) *partLoader {
	return &partLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO part (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaPartLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *partLoader {
	cols := []string{"P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"}
	return &partLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("part", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewPartSuppLoader(ctx context.Context, db *sql.DB, concurrency int) *partSuppLoader {
	return &partSuppLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO partsupp (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaPartSuppLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *partSuppLoader {
	cols := []string{"PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"}
	return &partSuppLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("partsupp", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewSuppLoader(ctx context.Context, db *sql.DB, concurrency int) *suppLoader {
	return &suppLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO supplier (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaSuppLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *suppLoader {
	cols := []string{"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"}
	return &suppLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("supplier", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewNationLoader(ctx context.Context, db *sql.DB, concurrency int) *nationLoader {
	return &nationLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO nation (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaNationLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *nationLoader {
	cols := []string{"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"}
	return &nationLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("nation", producer, idx, cols)
		}, concurrency), ctx,
	}}
}

func NewRegionLoader(ctx context.Context, db *sql.DB, concurrency int) *regionLoader {
	return &regionLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewSQLSink(db,
				`INSERT INTO region (R_REGIONKEY, R_NAME, R_COMMENT) VALUES `, 0, 0)
		}, concurrency), ctx}}
}

func NewKafkaRegionLoader(ctx context.Context, producer *kafka.Producer, concurrency int) *regionLoader {
	cols := []string{"R_REGIONKEY", "R_NAME", "R_COMMENT"}
	return &regionLoader{sqlLoader{
		sink.NewConcurrentSink(func(idx int) sink.Sink {
			return sink.NewKafkaSink("region", producer, idx, cols)
		}, concurrency), ctx,
	}}
}
