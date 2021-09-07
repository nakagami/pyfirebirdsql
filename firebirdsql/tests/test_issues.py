from firebirdsql.tests.base import *    # noqa


class TestIssues(TestBase):
    def test_issue_39(self):
        """
        .description attribute should be None when .execute has not run yet
        """
        cur = self.connection.cursor()
        self.assertEqual(None, cur.description)

    def test_issue_40(self):
        cur = self.connection.cursor()
        cur.execute("SELECT RDB$INDEX_NAME FROM RDB$INDICES WHERE RDB$INDEX_NAME LIKE 'RDB$INDEX_%'")
        self.assertNotEqual(None, cur.fetchone())
        cur.close()
        cur = self.connection.cursor()
        cur.execute("SELECT RDB$INDEX_NAME FROM RDB$INDICES WHERE RDB$INDEX_NAME LIKE ?", ('RDB$INDEX_%', ))
        self.assertNotEqual(None, cur.fetchone())
        cur.close()

    def test_issue_41(self):
        self.connection.cursor().execute('''
              CREATE TABLE issue_41
              (
                  a       INTEGER,
                  b       VARCHAR(20)
              )
        ''')
        self.connection.commit()

        cur = self.connection.cursor()
        cur.execute("INSERT INTO issue_41 (a, b) VALUES (32767, 'FOO')")
        cur.execute("INSERT INTO issue_41 (a, b) VALUES (32768, 'BAR')")
        cur.close()
        cur = self.connection.cursor()
        cur.execute('''SELECT b FROM issue_41 WHERE a=?''', (32767, ))
        self.assertEqual(cur.fetchone()[0], 'FOO')
        cur.close()
        cur = self.connection.cursor()
        cur.execute('''SELECT b from issue_41 WHERE a=?''', (32768, ))
        self.assertEqual(cur.fetchone()[0], 'BAR')
        cur.close()

    def test_issue_54(self):
        cur = self.connection.cursor()
        self.assertEqual(cur.execute("select count(*) from rdb$relation_fields"), cur)

        cur.close()

    def test_issue_60(self):
        self.connection.cursor().execute("CREATE TABLE issue_60 (b BLOB SUB_TYPE 1)")
        self.connection.commit()

        cur = self.connection.cursor()
        for i in range(400):
            cur.execute("INSERT INTO issue_60 (b) VALUES ('')")
        cur.close()
        cur = self.connection.cursor()
        cur.execute("SELECT b from issue_60")
        self.assertEqual(len(cur.fetchall()), 400)
        cur.close()

    def test_issue_74(self):
        cur = self.connection.cursor()
        cur.execute("""CREATE TABLE FPI_MOVTO_MOVIMIENTOS(
    RFCEMPRESA varchar(20) NOT NULL,
    NOSUCURSAL integer NOT NULL,
    TIPO integer NOT NULL,
    SERIE varchar(5) NOT NULL,
    NODOCTO integer NOT NULL,
    LINEA integer NOT NULL,
    CODART varchar(20),
    NOMART varchar(80),
    CLAVEPRODSERV varchar(10),
    UNIDADCLAVE varchar(10),
    UNIDADNOMBRE varchar(80),
    CANT1 double precision,
    CATN2 double precision,
    PUNIT double precision,
    MONTO double precision,
    IMPTO1 double precision,
    IMPTO2 double precision,
    PIMPTO1 double precision,
    PIMPTO2 double precision,
    TIMPTO1 varchar(10),
    TIMPTO2 varchar(10),
    TFIMPTO1 varchar(10),
    TFIMPTO2 varchar(10),
    PDESCTO double precision,
    IDESCTO double precision,
    CONSTRAINT PXFPI_MOVTO_MOVIMIENTOS PRIMARY KEY (RFCEMPRESA,NOSUCURSAL,TIPO,SERIE,NODOCTO,LINEA)
)""")
        self.connection.commit()
        cur.execute("""INSERT INTO FPI_MOVTO_MOVIMIENTOS (
    RFCEMPRESA,
    NOSUCURSAL,
    TIPO,
    SERIE,
    NODOCTO,
    LINEA,
    CODART,
    NOMART,
    CLAVEPRODSERV,
    UNIDADCLAVE,
    UNIDADNOMBRE,
    CANT1,
    CATN2,
    PUNIT,
    MONTO,
    IMPTO1,
    IMPTO2,
    PIMPTO1,
    PIMPTO2,
    TIMPTO1,
    TIMPTO2,
    TFIMPTO1,
    TFIMPTO2,
    PDESCTO,
    IDESCTO) VALUES (
    'p2',
    '0',
    '700',
    'X',
    '1',
    '1',
    'ART-001',
    'PRUEBA DE ARTICULO',
    '01010101',
    'ACT',
    'Actividad',
    '10.000000',
    '0.000000',
    '2.500000',
    '25.000000',
    '4.000000',
    '0.000000',
    '16.000000',
    '0.000000',
    '002',
    '',
    'Tasa',
    '',
    '0.000000',
    '0.000000')""")
        self.connection.commit()
        cur.execute("""
SELECT doc.RFCEMPRESA, doc.NOSUCURSAL, doc.TIPO, doc.SERIE, doc.NODOCTO, doc.LINEA,
    doc.CODART, doc.NOMART, doc.CLAVEPRODSERV, doc.UNIDADCLAVE, doc.UNIDADNOMBRE, doc.CANT1,
    doc.CATN2, doc.PUNIT, doc.MONTO, doc.IMPTO1, doc.IMPTO2, doc.PIMPTO1, doc.PIMPTO2,
    doc.TIMPTO1, doc.TIMPTO2, doc.TFIMPTO1, doc.TFIMPTO2, doc.PDESCTO, doc.IDESCTO
FROM FPI_MOVTO_MOVIMIENTOS doc
WHERE doc.RFCEMPRESA = 'p2' and doc.NOSUCURSAL = 0 and doc.TIPO = 700 and doc.SERIE = 'X' and doc.NODOCTO = 1""")
        self.assertEqual(len(cur.description), 25)
        self.assertEqual(len(cur.fetchall()), 1)

        cur.close()
