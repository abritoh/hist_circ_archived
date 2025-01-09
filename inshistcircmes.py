# -*- coding: utf-8 -*-
"""
-- ============================================================================
-- Descripción....: 
	(1) Inserta datos en la tabla [siapcc_pe_dm_circhist_mes]:         
    
-- Elabora........: ArBR (arcebrito@gmail.com)
-- Fecha..........: 2019-02-07
---                 2019-02-27 ─ Se parametriza tabla destino V1 o V2
-- ============================================================================
"""

import time
import utlpy
import pymysql
from threading import Thread

PR_NAME = "inshistcircmes"
CN = {"host": "10.4.22.84", "user": "ClusterBR", "passwd": "XYX123...", "database": "apcc"}

TB_DM_CIRCHIST_MES = {'V1':'siapcc_pe_dm_circhist_mes', 'V2': 'siapcc_pe_dm_circhist_mes_2'}

def execute_sql(connection, anio, mes, version) :
    
    cursor = None
    try:
        cursor = pymysql.cursors.Cursor(connection)
        sql_delete = "delete from apcc.[TB_DM_CIRCHIST_MES] where mes = '[MES]' and anio = '[ANIO]'"        
        sql_delete = sql_delete.replace("[TB_DM_CIRCHIST_MES]", TB_DM_CIRCHIST_MES[version])
        sql_delete = sql_delete.replace("[MES]", mes)
        sql_delete = sql_delete.replace("[ANIO]", anio)    
        cursor.execute(sql_delete)
        connection.commit()
            
        sql_insert = """            
                insert ignore into apcc.[TB_DM_CIRCHIST_MES] 
                	(cve, cveDivision, cveZona, cveSubestacion, circuito, anio, mes,  
                    demandaMax, fp, nivelVoltOp, energia, demReactiva, demMedia, facCarga)    
                	select distinct concat(h.chdiv, h.chzona, h.chsub, h.chcir, h.chultact, h.chmes) as cve,
                		h.chdiv, h.chzona, h.chsub, h.chcir, h.chultact, h.chmes, 
                		h.chdem, h.chfp, null, h.chener, h.chdemreac, h.chdemmed, h.chfaccar
                		from apcc.dm_circhist h 
                			where h.chultact = '[ANIO]' and h.chmes = '[MES]'
                			and not exists (select *
                							from apcc.[TB_DM_CIRCHIST_MES] hm
                							where hm.cveDivision = h.chdiv and hm.cveZona = h.chzona 
                							and hm.cveSubestacion = h.chsub and hm.circuito = h.chcir
                                            and hm.anio = h.chultact and hm.mes = h.chmes)
                """        
        
        sql_insert = sql_insert.replace("[TB_DM_CIRCHIST_MES]", TB_DM_CIRCHIST_MES[version])
        sql_insert = sql_insert.replace("[MES]", mes)
        sql_insert = sql_insert.replace("[ANIO]", anio)
        cursor.execute(sql_insert)
        connection.commit()
        
    except Exception as e:
        utlpy.println('execute_sql.error {} >>> {}'.format(sql_insert, str(e)))
    finally:
        if cursor:
            cursor.close()    
    return


def fn_execute_proccess(anio, mes, version) :
    
    start_time = time.time()
    prid = utlpy.btc_gen_prid(PR_NAME, "{}{}".format(anio, mes))

    connection = None
    try:
        connection = pymysql.connect(host=CN["host"], user=CN["user"], passwd=CN["passwd"], database=CN["database"], autocommit=True)    
        utlpy.btc_insert(connection, prid, PR_NAME, "INICIADO", "", "*", "*", "", "", anio, mes)
        
        str_cve = "{} {}".format(anio, mes)
        utlpy.println("fn_execute_thread: {} thread started & running (...)".format(str_cve))
        
        execute_sql(connection, anio, mes, version)        
        
        elapsed_time = time.time() - start_time
        elapsed_time_fmt = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        
        message = "DURACION {}".format(str(elapsed_time_fmt))        
        utlpy.println("fn_execute_thread: {} thread completed. Total time: {}".format(str_cve, elapsed_time_fmt))
        utlpy.btc_insert(connection, prid, PR_NAME, "COMPLETADO", message, "*", "*", "", "", anio, mes)
        
    except Exception as e:
        utlpy.println('fn_execute_thread.error {} >>> {}'.format(str_cve, str(e)))
        if connection:
            utlpy.btc_insert(connection, prid, PR_NAME, "ERROR", str(e), "*", "*", "", "", anio, mes)        
    finally:    
        if connection:
            connection.close()
    return


def fn_execute_foreach(anio, lst_months, useThread, version) :
    utlpy.println("fn_execute_foreach_thread:started")

    try:
        lst_threads = []                
        for mes in lst_months : 
            if useThread:
                t = Thread(target = fn_execute_proccess, args = (anio, mes, version))
                lst_threads.append(t)
            else:
                fn_execute_proccess(anio, mes, version)
        
        if useThread:
            [t.start() for t in lst_threads]
            [t.join() for t in lst_threads]
            
    except Exception as e:
        utlpy.println('fn_execute_foreach_thread.error>>> {}'.format(str(e)))
        
    print("fn_execute_foreach_thread:main thread completed")
    return


##########################
# main
##########################

if __name__ == '__main__' :
    
    start_time = time.time()
    anio = "2018"    
    lst_months = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC']
    
    fn_execute_foreach(anio, lst_months, False, 'V1')
    elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
    print("__main__ completed, duration:{}".format(elapsed_time_fmt))
    
        