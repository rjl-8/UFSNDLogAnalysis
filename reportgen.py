#!/usr/bin/env python3
import psycopg2


def get_rep1():
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    sql = '''\
    select '"' || articles.title || '"', count(*) || ' views'
    from articles
        join log
            on '/article/' || articles.slug = log.path
    group by articles.title
    order by count(*) desc
    limit 3;
    '''
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    return results


def get_rep2():
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    sql = '''\
    select authors.name, count(*) || ' views'
    from articles
        join authors
            on articles.author = authors.id
        join log
            on '/article/' || articles.slug = log.path
    group by authors.name
    order by count(*) desc;
    '''
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    return results


def get_rep3():
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    sql = '''\
    select err_date
         , to_char(err_perc, '999D99%')
    from (
          select trim(to_char(time, 'Month'))
                 || ' ' || trim(to_char(time, 'DD, YYYY')) err_date
               , 100.*count(case
                            when status != '200 OK' then 1
                            else null
                            end)
                 /count(*) err_perc
          from log
          group by trim(to_char(time, 'Month'))
                   || ' ' || trim(to_char(time, 'DD, YYYY'))
         ) src
    where err_perc >= 1.
    order by err_perc desc;
    '''
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    return results

REP1_WRAP = '''\
The Most Popular Articles of All Time!
--------------------------------------
%s

'''

REP2_WRAP = '''\
The Most Popular Authors of All Time!
-------------------------------------
%s

'''

REP3_WRAP = '''\
Error Report - Days with Over 1%% Error
--------------------------------------
%s

'''

RES_WRAP = '''\
%s - %s
'''

REP1_RES = "".join(RES_WRAP % (elem1, elem2) for elem1, elem2 in get_rep1())
REP2_RES = "".join(RES_WRAP % (elem1, elem2) for elem1, elem2 in get_rep2())
REP3_RES = "".join(RES_WRAP % (elem1, elem2) for elem1, elem2 in get_rep3())

final = REP1_WRAP % REP1_RES + REP2_WRAP % REP2_RES + REP3_WRAP % REP3_RES

print (final)
