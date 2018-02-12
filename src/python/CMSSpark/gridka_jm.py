#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=

"""
Spark script to parse JobMonitoring records on HDFS.
"""

# system modules
import calendar
import datetime
import time
import argparse
import re

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit
from pyspark.sql.types import DoubleType, IntegerType

# CMSSpark modules
from CMSSpark.schemas import schema_jm
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, jm_tables, split_dataset
from CMSSpark.spark_utils import avro_rdd
from CMSSpark.utils import elapsed_time 


class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process JobMonitoring metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms/jm-data-popularity/avro-snappy'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'jm_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select JobMonitoring data for specific date (YYYYMMDD)')
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")


def jm_date(date):
    "Convert given date into JobMonitoring date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)


def jm_date_unix(date):
    "Convert JobMonitoring date into UNIX timestamp"
    return time.mktime(time.strptime(date, 'year=%Y/month=%m/day=%d'))

# global patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')
PAT_MM = re.compile(r'^(0[1-9]|1[012])$')
PAT_DD = re.compile(r'^(0[1-9]|[12][0-9]|3[01])$')


def dateformat(value):
    """Return seconds since epoch for provided YYYYMMDD or number with suffix 'd' for days"""
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD or number with suffix 'd' for days"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            year = value[0:4]
            if  not PAT_YYYY.match(year):
                raise Exception(msg + ', fail to parse the year part, %s' % year)
            month = value[4:6]
            date = value[6:8]
            ddd = datetime.date(int(year), int(month), int(date))
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    elif value.endswith('d'):
        try:
            days = int(value[:-1])
        except ValueError:
            raise Exception(msg)
        return time.time()-days*24*60*60
    else:
        raise Exception(msg)


def range_dates(trange):
    "Provides dates range in HDFS format from given list"
    out = [jm_date(str(trange[0]))]
    if  trange[0] == trange[1]:
        return out
    tst = dateformat(trange[0])
    while True:
        tst += 24*60*60
        tdate = time.strftime("%Y%m%d", time.gmtime(tst))
        out.append(jm_date(tdate))
        if  str(tdate) == str(trange[1]):
            break
    return out


def hdfs_path(hdir, dateinput):
    "Construct HDFS path for JM data"
    dates = dateinput.split('-')
    if  len(dates) == 2:
        return ['%s/%s' % (hdir, d) for d in range_dates(dates)]
    dates = dateinput.split(',')
    if  len(dates) > 1:
        return ['%s/%s' % (hdir, jm_date(d)) for d in dates]
    return ['%s/%s' % (hdir, jm_date(dateinput))]


def run(date, fout, hdir, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    def create_jm_table(ctx, sqlContext, hdir=hdfs_path(hdir, date), date=None, verbose=None):
        rdd = avro_rdd(ctx, sqlContext, hdir, date, verbose)
        jdf = sqlContext.createDataFrame(rdd, schema=schema_jm())
        df = jdf.withColumn("WrapWC", jdf["WrapWC"].cast(DoubleType())) \
                .withColumn("WrapCPU", jdf["WrapCPU"].cast(DoubleType())) \
                .withColumn("ExeCPU", jdf["ExeCPU"].cast(DoubleType())) \
                .withColumn("NCores", jdf["NCores"].cast(IntegerType())) \
                .withColumn("NEvProc", jdf["NEvProc"].cast(IntegerType())) \
                .withColumn("NEvReq", jdf["NEvReq"].cast(IntegerType()))
        df.registerTempTable('jm_df')
        tables = {'jm_df': df}

        # Cache data in memory and disk
        df.persist(StorageLevel.MEMORY_AND_DISK)

        return tables

    create_jm_table(ctx, sqlContext, date='', verbose=verbose)

    # Query JobMonitoring data
    class QueryBuild(object):
        def __init__(self, table_name, columns='*', conditions=None):
            self.attributes = {'columns': columns, 'conditions': conditions, 'table_name': table_name}
            self.convert_functions = {'columns': self.convert_columns,
                                      'conditions': self.convert_conditions,
                                      'table_name': lambda x: x}

        def __getattr__(self, item):
            return self.convert_functions[item](self.attributes[item])

        @staticmethod
        def convert_columns(columns):
            if isinstance(columns, list):
                if len(columns) > 1:
                    return ','.join(columns)
                else:
                    return columns[0]
            else:
                return columns

        def convert_conditions(self, conditions):
            first_item = True
            condition_string = ''
            for column, condition in conditions.iteritems():
                if first_item:
                    condition_string = 'WHERE {}.{}="{}"'.format(self.attributes['table_name'], column, condition)
                    first_item = False
                else:
                    condition_string += ' AND {}.{}="{}"'.format(self.attributes['table_name'], column, condition)
            return condition_string

    query_attributes = QueryBuild('jm_df', ['TaskMonitorId', 'WNHostName', 'Application', 'ApplicationVersion',
                                            'NCores', 'JobType', 'Type', 'GenericType', 'SubmissionTool', 'NEvProc',
                                            'NEvReq', 'WrapCPU', 'WrapWC', 'StartedRunningTimeStamp',
                                            'FinishedTimeStamp'], {'SiteName': 'T1_DE_KIT'})

    stmt = 'SELECT {q.columns} FROM {q.table_name} {q.conditions}'.format(q=query_attributes)

    print(stmt)

    query = sqlContext.sql(stmt)
    print_rows(query, stmt, verbose)

    # keep table around
    query.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        query.write.format("com.databricks.spark.csv").option("header", "true").save(fout)

    ctx.stop()


def main():
    """Main function"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    run(opts.date, opts.fout, opts.hdir, opts.yarn, opts.verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))


if __name__ == '__main__':
    main()
