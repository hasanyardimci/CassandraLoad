import time
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider


class CassandraSchema:

    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        ap = PlainTextAuthProvider(username='cassandra', password='cassandra')
        self.cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=ap)
        self.session = self.cluster.connect(self.keyspace)
        return self.session

    def getsession(self):
        return self.session

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace):
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            print("dropping existing keyspace...")
            self.session.execute("DROP KEYSPACE " + keyspace)
            print("creating keyspace...")
            '''
            self.session.execute("""
                            CREATE KEYSPACE %s
                            with 
                            replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
                            """ % keyspace)
            '''
            self.session.execute("""
                CREATE KEYSPACE %s
                with 
                replication = { 'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3' }
                """ % keyspace)

        else:
            print("creating keyspace...")
            '''
            self.session.execute("""
                            CREATE KEYSPACE %s
                            with 
                            replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
                            """ % keyspace)
            '''
            self.session.execute("""
                            CREATE KEYSPACE %s
                            with 
                            replication = { 'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3' }
                            """ % keyspace)

        print("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_table(self, keyspace):
        self.session.set_keyspace(keyspace)
        c_sql = """
                CREATE TABLE IF NOT EXISTS employee (emp_id int,
                                              ename varchar,
                                              sal double,
                                              city varchar,
                                              primary key (emp_id))
                 with compression = {'sstable_compression': '', 'chunk_length_kb': 64};
                 """
        self.session.execute(c_sql)
        print("Employee Table Created !!!")

    def insert_data_batch(self, keyspace):
        self.session.set_keyspace(keyspace)
        insert_sql = self.session.prepare("INSERT INTO  employee (emp_id, ename , sal,city) VALUES (?,?,?,?)")
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for k in range(1, 20):
            batch.add(insert_sql, [k, 'Reading can open your mind to brilliant new worlds and take you to a new level of English \
                                                language learning.It may feel like a slow process, but it is effective.Adopting English \
                                                Reading can open your mind to brilliant new worlds and take you to a new level of English \
                                                Reading can open your mind to brilliant new worlds and take you to a new level of English \
                                                Reading can open your mind to brilliant new worlds and take you to a new level of English \
                                                Reading can open your mind to brilliant new worlds and take you to a new level of English \
                                                books as learning tools can help you reach English fluency faster than ever before.',
                                   k, 'Dubai'])
            self.session.execute(batch)
            # print('Batch Insert Completed')
        print('Batch Insert Totally Completed')

    def insert_data_one(self, keyspace, fs, ls):
        self.session.set_keyspace(keyspace)
        insert_sql = SimpleStatement("INSERT INTO employee (emp_id, ename , sal,city) VALUES (%s,%s,%s,%s)",
                                     consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for i in range(fs, ls):
            first_number = (i - 1) * 10
            last_number = i * 10
            while first_number < last_number:
                self.session.execute(insert_sql, [first_number, 'Reading can open your mind to brilliant new worlds and take you to a new level of English\
                                                       Reading can open your mind to brilliant new worlds and take you to a new level of English\
                                                       Reading can open your mind to brilliant new worlds and take you to a new level of English\
                                                       Reading can open your mind to brilliant new worlds and take you to a new level of English\
                                                       Reading can open your mind to brilliant new worlds and take you to a new level of English',
                                                  first_number, 'Dubai'])
                first_number = first_number + 1
                self.session.execute(insert_sql, [k,'Reading can open your mind to brilliant new worlds and take you to a new level of English',k, 'Dubai'])
        print('Insert Totally Completed')

    def select_data(self, keyspace):
        self.session.set_keyspace(keyspace)
        rows = self.session.execute('select * from employee limit 1')
        for row in rows:
            print(row.emp_id)


def my_main(keyspace, fs, ls):
    cs = CassandraSchema()
    cs.createsession()
    t1 = time.time()
    t3 = time.time()
    cs.insert_data_one(keyspace, fs, ls)
    t2 = time.time()
    print(t1, t2, t2 - t1, t3)
    # cs.select_data(keyspace)


keyspace = input('keyspace : ') or 'backup_test'
fs = int(input('fs : '))
ls = int(input('ls : '))
my_main(keyspace, fs, ls)