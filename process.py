import urllib2
import csv
import psycopg2

class MigrationDownload:
    
    def __init__(self):
        
        self.conn = psycopg2.connect("dbname='research'")
        self.cursor = self.conn.cursor()
        self.fips = {
            '00': {'name': 'Total', 'counties': {'000': 'U.S.', }},
            '96': {'name': 'Total', 'counties': {'000': 'All migration', }},
            '97': {'name': 'Total', 'counties': {'000': 'U.S.', '001': 'Same state', '003': 'Different state' }},
            '98': {'name': 'Total', 'counties': {'000': 'Foreign' }},
        }

    def download(self):
        
        sql = ["""DROP TABLE IF EXISTS migration_in""",
        """CREATE TABLE migration_in (year int, state_dest varchar(2), county_dest varchar(3),    
        state_origin varchar(2), county_origin varchar(3), fam_num int, indivs_num int, income_num int,
        fam_income float, fam_size float)""", 
        """ALTER TABLE migration_in ADD CONSTRAINT migration_in_key UNIQUE (year,state_dest,county_dest,state_origin,county_origin)""",
        """DROP TABLE IF EXISTS migration_out""",
        """CREATE TABLE migration_out (year int, state_origin varchar(2), county_origin varchar(3),    
        state_dest varchar(2), county_dest varchar(3), fam_num int, indivs_num int, income_num int,
        fam_income float, fam_size float)""", 
        
        """ALTER TABLE migration_out ADD CONSTRAINT migration_out_key UNIQUE (year,state_dest,county_dest,state_origin,county_origin)""",
        """DROP TABLE IF EXISTS migration_fips""",
        """CREATE TABLE migration_fips (fips varchar(5), state varchar(2) NOT NULL, county varchar(3) NOT NULL, 
        county_name varchar(50), state_name varchar(30))""",
        """ALTER TABLE migration_fips ADD CONSTRAINT fips_key PRIMARY KEY (fips)""",
        
        """DROP VIEW IF EXISTS migration_inout""",
        """CREATE VIEW migration_inout AS
        SELECT COALESCE(i.year, o.year) record_year,
        COALESCE(i.state_dest, o.state_origin) in_state_dest,
        COALESCE(i.county_dest, o.county_origin) in_county_dest,
        COALESCE(o.state_dest, i.state_origin) out_state_dest,
        COALESCE(o.county_dest, i.county_origin) out_county_dest,
        i.fam_num in_fam, i.indivs_num in_indivs, i.income_num in_income, i.fam_income in_fam_income, i.fam_size in_fam_size,
        o.fam_num out_fam, o.indivs_num out_indivs, o.income_num out_income, o.fam_income out_fam_income, o.fam_size out_fam_size
         FROM migration_in i FULL OUTER JOIN migration_out o ON (i.year=o.year AND i.state_dest=o.state_origin AND i.county_dest=o.county_origin AND i.state_origin=o.state_dest AND i.county_origin=o.county_dest);""",
         """DROP VIEW IF EXISTS migration_withnames""",
         """CREATE VIEW migration_withnames AS
         SELECT m.record_year, fips_in.fips in_fips, fips_out.fips out_fips, fips_in.county_name in_dest_county, fips_in.state_name in_dest_state, fips_out.county_name out_dest_county, fips_out.state_name out_dest_state, 
         in_fam, in_indivs, in_income, in_fam_income,in_fam_size,
        out_fam, out_indivs, out_income, out_fam_income, out_fam_size,
        COALESCE(in_fam,0)-COALESCE(out_fam,0) bal_fam,
        COALESCE(in_income,0)-COALESCE(out_income,0) bal_income,
        COALESCE(in_indivs,0)-COALESCE(out_indivs,0) bal_indivs,
        in_fam_size-out_fam_size bal_fam_size, 
        in_fam_income-out_fam_income bal_fam_income
         FROM migration_inout m LEFT JOIN migration_fips fips_in ON (m.in_state_dest=fips_in.state AND m.in_county_dest=fips_in.county) LEFT JOIN migration_fips fips_out ON (m.out_state_dest=fips_out.state AND m.out_county_dest=fips_out.county)""" ]
         
        for statement in sql:
            self.cursor.execute(statement)
            self.conn.commit()

        #add new years here as necessary
        years = ['0405','0506','0607','0708','0809']
        for year in years:

            read = csv.reader( urllib2.urlopen('http://www.irs.gov/pub/irs-soi/countyinflow%s.csv' % year))
            read.next() #skip header
            for row in read:
                if row[0:2]!=row[2:4]:
                    self.add_fips( row[2:6] )
                outrow = self.processrow(year,row)
                if outrow:
                    sql = "INSERT INTO migration_in VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute( sql, outrow )
            self.conn.commit()

            read = csv.reader( urllib2.urlopen('http://www.irs.gov/pub/irs-soi/countyoutflow%s.csv' % year))
            read.next() #skip header
            for row in read:
                if row[0:2]!=row[2:4]:
                    self.add_fips( row[2:6] )
                outrow = self.processrow(year,row)
                if outrow:
                    sql = "INSERT INTO migration_out VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute( sql, outrow )
            self.conn.commit()
            
        for state in self.fips.keys():
            for county in self.fips[state]['counties'].keys():
                fips = state+county
                sql = "INSERT INTO migration_fips VALUES (%s,%s,%s,%s,%s)"
                row = (fips,state,county,self.fips[state]['counties'][county].decode('UTF-8','replace'),self.fips[state]['name'])
                self.cursor.execute( sql, row )        
        self.conn.commit()     

    def processrow(self,year,row):
                
        #State_Code_Dest,County_Code_Dest,State_Code_Origin,County_Code_Origin,State_Abbrv,County_Name,Return_Num,Exmpt_Num,Aggr_AGI
        try:
            families = int(row[6])
            fam_income = float(row[8])/families
            fam_size = float(row[7])/families
        except:
            families, fam_income, fam_size = None, None, None
        try:
            outrow = [year, row[0], row[1], row[2], row[3], row[6], row[7], row[8], fam_income, fam_size]
        except:
            return None
        return outrow

    def add_fips(self,code):
        state, county, state_name, county_name = code
        if state in ['96','97','98','99']:
            return
        if state not in self.fips:
            self.fips[state] =  {'name': state_name, 'counties': {}}
        self.fips[state]['counties'][county] = county_name


if __name__ == '__main__':
    downloader = MigrationDownload()
    downloader.download()



 
 
 
 
