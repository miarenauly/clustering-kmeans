from sklearn.cluster import KMeans
from mysql.connector import connect, Error

dbhost = 
dbuser = 
dbpass = 
dbname = 
dbport = 

conn = connect(host=dbhost,
               user=dbuser,
               password=dbpass,
               database=dbname,
               port=dbport)


def fetch_data(sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        return data
    except Error:
        pass


def execScalar(sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        rowsaffected = cursor.rowcount
    except:
        rowsaffected = -1
        conn.rollback()
    cursor.close()
    return rowsaffected


def clustering(data, n_cluster):
    longlat = []
    for row in data:
        lng = row[4]
        lat = row[5]
        a_data = [lng, lat]
        longlat.append(a_data)

    kmeans = KMeans(n_clusters=n_cluster, random_state=0)
    kmeans.fit(longlat)
    labels = kmeans.predict(longlat)
    labels = labels.tolist()
    return labels


def create_table(new_table_name):
    sql = '''CREATE TABLE %s (
	DayID varchar(255) DEFAULT NULL,
	FSRCode varchar(255) DEFAULT NULL,
	FSRName varchar(255) DEFAULT NULL,
	cust_id varchar(255) DEFAULT NULL,
	LNG double DEFAULT NULL,
	LAT double DEFAULT NULL,
	FSRArea varchar(255) DEFAULT NULL,
	Cluster varchar(255) DEFAULT NULL,
	di_id varchar(255) DEFAULT NULL
	)
	''' % (new_table_name)
    return sql


def clustering_day(table_name):
    # clustering
    sql = "select * from %s" % (table_name)
    data = fetch_data(sql)
    n_cluster = 12
    cluster = clustering(data, n_cluster)
    # making table
    new_table_name = 'cluster_day'
    sql = create_table(new_table_name)
    execScalar(sql)
    # insert data
    sql = "select * from %s" % (table_name)
    data = fetch_data(sql)
    n = 0
    for row in data:
        DayID = cluster[n]
        n += 1
        FSRCode = row[1]
        FSRName = row[2]
        cust_id = row[3]
        LNG = row[4]
        LAT = row[5]
        FSRArea = row[6]
        di_id = row[8]

        sql = '''insert into %s (DayID, FSRCode, FSRName, cust_id, LNG, LAT, FSRArea, di_id)
			values ('%s','%s','%s','%s',%s, %s,'%s','%s')''' % (
            new_table_name, DayID, FSRCode, FSRName, cust_id, LNG, LAT, FSRArea, di_id)
        print(sql)
        execScalar(sql)


def clustering_fsr(table_name):
    i = 11
    while i >= 0:
        # clustering
        sql = "select * from %s where DayID = '%s'" % (table_name, i)
        data = fetch_data(sql)
        n_cluster = 16
        cluster = clustering(data, n_cluster)
        # making table
        new_table_name = 'cluster_fsr'
        sql = create_table(new_table_name)
        execScalar(sql)
        # insert data
        sql = "select * from %s where DayID = '%s'" % (table_name, i)
        data = fetch_data(sql)
        i -= 1
        n = 0
        for row in data:
            Cluster = cluster[n]
            n += 1
            DayID = row[0]
            FSRCode = row[1]
            FSRName = row[2]
            cust_id = row[3]
            LNG = row[4]
            LAT = row[5]
            FSRArea = row[6]
            di_id = row[8]

            sql = '''insert into %s (DayID, FSRCode, FSRName, cust_id, LNG, LAT, FSRArea, Cluster, di_id)
				values ('%s','%s','%s','%s',%s,%s, '%s','%s','%s')''' % (
                new_table_name, DayID, FSRCode, FSRName, cust_id, LNG, LAT, FSRArea, Cluster, di_id)
            print(sql)
            execScalar(sql)


def main(table_master):
    clustering_day(table_master)
    clustering_fsr('cluster_day')

main('master_cluster_visualization')
