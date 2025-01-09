# -*- coding: utf-8 -*-
"""
-- ============================================================================
-- Descripción....: 
	(1) Limpia Atipicos y Obtiene parametrps de perfiles de carga:         
    
-- 
-- run C:\pythonPATH\python.exe C:\scriptPATH\siapcc_prod.py
--

-- Elabora........: arbr(threading version)
-- Fecha..........: 2019-01-30
-- ============================================================================
"""

import math
import time
import utlpy
import pymysql
import numpy as np
import calc_siapcc

from threading import Thread
from calendar import monthrange
from multiprocessing import Process

PR_NAME = calc_siapcc.PR_NAME
log = calc_siapcc.log
CN = calc_siapcc.CN
parametros_hist_mes = calc_siapcc.parametros_hist_mes
perfiles_horarios = calc_siapcc.perfiles_horarios

THREAD_JOIN_TIME_OUT_SEGS = 6 * 60
THREAD_MAIN_SEGS_SLEEP    = 20 * 60

sql_kw = """
    select id_equipo, agno, mes, hora, minuto,
    	sum(D1) as 'd1', sum(D2) as 'd2', sum(D3) as 'd3', sum(D4) as 'd4', sum(D5) as 'd5', sum(D6) as 'd6',
    	sum(D7) as 'd7', sum(D8) as 'd8', sum(D9) as 'd9', sum(D10) as 'd10', sum(D11) as 'd11', sum(D12) as 'd12',
    	sum(D13) as 'd13', sum(D14) as 'd14', sum(D15) as 'd15', sum(D16) as 'd16', sum(D17) as 'd17', sum(D18) as 'd18',
    	sum(D19) as 'd19', sum(D20) as 'd20', sum(D21) as 'd21', sum(D22) as 'd22', sum(D23) as 'd23', sum(D24) as 'd24',
    	sum(D25) as 'd25', sum(D26) as 'd26', sum(D27) as 'd27', sum(D28) as 'd28', sum(D29) as 'd29', sum(D30) as 'd30',
    	sum(D31) as 'd31' from hpe_kw_extend HPEE
    	where HPEE.id_equipo = %s
    	and year(HPEE.fecha) = %s and month(HPEE.fecha) = %s
    	group by id_equipo, hora, minuto 
    	order by id_equipo, hora, minuto """

sql_kvar = """
    select id_equipo, agno, mes, hora, minuto,
    	sum(D1) as 'd1', sum(D2) as 'd2', sum(D3) as 'd3', sum(D4) as 'd4', sum(D5) as 'd5', sum(D6) as 'd6',
    	sum(D7) as 'd7', sum(D8) as 'd8', sum(D9) as 'd9', sum(D10) as 'd10', sum(D11) as 'd11', sum(D12) as 'd12',
    	sum(D13) as 'd13', sum(D14) as 'd14', sum(D15) as 'd15', sum(D16) as 'd16', sum(D17) as 'd17', sum(D18) as 'd18',
    	sum(D19) as 'd19', sum(D20) as 'd20', sum(D21) as 'd21', sum(D22) as 'd22', sum(D23) as 'd23', sum(D24) as 'd24',
    	sum(D25) as 'd25', sum(D26) as 'd26', sum(D27) as 'd27', sum(D28) as 'd28', sum(D29) as 'd29', sum(D30) as 'd30',
    	sum(D31) as 'd31' from hpe_kvar_extend HPEE 
    	where HPEE.id_equipo = %s
    	and year(HPEE.fecha) = %s and month(HPEE.fecha) = %s
    	group by id_equipo, hora, minuto 
    	order by id_equipo, hora, minuto """

sql_energia = """
    select id_equipo, year(fecha) as anio, month(fecha) as mes, 
    	sum(kwhe) as EnergiaMensualE, sum(kwhr) as EnergiaMensualR , 
    	sum(kwhe)/DAYOFMONTH(LAST_DAY(fecha)) as EnergiaMediaE, 
        sum(kwhr)/DAYOFMONTH(LAST_DAY(fecha)) as EnergiaMediaR, 
        DAYOFMONTH(LAST_DAY(fecha)) as diasMes,
    	sum(q1) as q1, 
        sum(q2) as q2, 
        sum(q3) as q3, 
        sum(q4) as q4,
        max(q1) as maxq1, 
        sum(q2) as maxq2, 
        sum(q3) as maxq3, 
        sum(q4) as maxq4
        from apcc.historicos_energia
        where id_equipo = %s and year(fecha) = %s and month(fecha) = %s
        group by year(fecha), month(fecha) """
        
sql_nivel_vop = """
    select ifnull(avg(nullif(hpe.vav,0)), 0) from historicos_parametros_electricos hpe 
            where id_equipo = %s
            and year(hpe.fecha) = %s and month(hpe.fecha) = %s"""


def fn_calcula_perfiles_horarios_por_equipo(guardarEnBD, cve, agno, tipo_equipo, id_equipo, lst_meses):
    
    start_time = utlpy.current_time()
    str_meses = ",".join(lst_meses)
    log.info(">>> Started {} Meses: {}. {} ID={}".format(tipo_equipo, str_meses, cve, id_equipo))
    
    connection = None
    cursor = None
    try:
        connection = pymysql.connect(host=CN["host"], user=CN["user"], passwd=CN["passwd"], database=CN["database"], charset=CN["charset"])
        cursor = pymysql.cursors.Cursor(connection)
        
        for imes in range(12):        
            
            mes = imes + 1
            smes = utlpy.smes(mes)
            
            if not smes in lst_meses:
                continue
            
            start_time_mes = utlpy.current_time()        
            start_time_mes_date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            log.info(">>>>>> Started. {}: {} ID={}, Mes:{}, At: {}".format(tipo_equipo, cve, id_equipo, mes, start_time_mes_date))
            
            diasMes = monthrange(agno, mes)
            numcols = diasMes[1]
            
            numrows = cursor.execute(sql_kw, (id_equipo, agno, mes))
            result_kw = cursor.fetchall()
            datosInikW = np.asarray(result_kw, dtype=float)
            
            lts_insert = []
            if numrows == 0:
                #print ('result_kw => 0')
                for diames in range(numcols):
                    for row in range(24):
                        fecha = "{}-{}-{}".format(str(agno), str(mes), str(int(diames+1)))
                        hora = str(row + 1) + ":00:00"
                        lts_insert.append((id_equipo, fecha, hora))
                        #print("Empty record inserted >> {} {} {}".format(id_equipo, fecha, hora))
                #end for dia
                
                if guardarEnBD :
                    smes = utlpy.smes(mes)
                    param_hist_mes = (id_equipo, str(agno), str(mes), smes, -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1)
                    
                    sql_ins_hm = calc_siapcc.SQL_INS_HIST_MES.replace("[parametros_hist_mes]", parametros_hist_mes[tipo_equipo])	
                    cursor.execute(sql_ins_hm, param_hist_mes)
                    connection.commit()                
                    sql_ins_ph_null = """INSERT INTO apcc.[perfiles_horarios]
                                            (id_equipo, fecha, hora, kw, kvar) VALUES (%s, %s, %s, null, null);"""
                    sql_ins_ph_null = sql_ins_ph_null.replace("[perfiles_horarios]", perfiles_horarios[tipo_equipo])
    				
                    cursor.executemany(sql_ins_ph_null, lts_insert)
                    connection.commit()            
                #end if guardarEnBD
            else:
                datosReskW = calc_siapcc.limpiaAtipicos (agno, mes, id_equipo, result_kw, numrows)      	
                numrows = cursor.execute(sql_kvar, (id_equipo, agno, mes))
                result_kvar = cursor.fetchall()
                datosInikVAR = np.asarray(result_kvar, dtype=float)                
                datosReskVAR = calc_siapcc.limpiaAtipicos (agno, mes, id_equipo, result_kvar, numrows)
                datosIniEnergia = -1
                
                cursor.execute(sql_nivel_vop, (id_equipo, agno, mes))            
                result_voper = cursor.fetchone()           
                
                voper = -1
                if result_voper and len(result_voper)>=1 and result_voper[0] :
                    voper = utlpy.truncate(result_voper[0], 4)
                
                try:
                    calc_siapcc.obtenerParametros (guardarEnBD, connection, cursor, tipo_equipo, id_equipo, agno, mes, datosReskW, datosReskVAR, datosInikW, datosInikVAR, datosIniEnergia, numcols, diasMes, voper)
                except Exception as errparam:
                    log.error("obtenerParametros.error>>> {}".format(str(errparam)), exc_info=True)
            
            elapsed_time_fmt_mes = utlpy.elapsed_time_fmt(start_time_mes)
            log.info(">>>>>> Completed {}: {} ID={}, Mes:{}, Time: {}".format(tipo_equipo, cve, id_equipo, mes, elapsed_time_fmt_mes))
        #end for mes
            
        elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
        log.info(">>> Completed {}. Meses: {}. {} ID={} Time: {}".format(tipo_equipo, str_meses, cve, id_equipo, elapsed_time_fmt))
            
    except Exception as e:
        log.error("fn_cal_per_hor_eq.error>>> {} {} {}".format(cve, id_equipo, str(e)), exc_info=True)
        
    finally:
        if cursor:
            cursor.close()
            del cursor
        if connection:
            connection.close()
            del connection
    
    return


def proceso_calcula_perfiles_horarios(guardarEnBD, useThreads, sql_base, MAX_THREADS, anio, tipo_equipo, lst_div, lst_meses) :
    log.info("calc_perf_hr:started")
    
    ianio = int(anio)    
    
    connection = None
    cursor = None    
    try:        
        connection = pymysql.connect(host=CN["host"], user=CN["user"], passwd=CN["passwd"], database=CN["database"], charset=CN["charset"], autocommit=False)
        cursor = pymysql.cursors.Cursor(connection)
        
        cursor.execute(sql_base)
        arr_base = cursor.fetchall()
        cursor.close()
        connection.close()
        cursor = None
        connection = None
        lst_equipos = [x[0] for x in arr_base]
            
        idx_group = 1
        total_groups = int(math.ceil(len(arr_base) / MAX_THREADS))
        
        log.info("[***>>>>>>] Formando {} Grupos de Ejecucion. Threads a Utilizar: {}\n".format(total_groups, MAX_THREADS))
        
        while idx_group <= total_groups:
            
            limit_1 = (idx_group - 1) * MAX_THREADS
            limit_2 = (idx_group) * MAX_THREADS - 1
            arr_rango = arr_base[limit_1 : limit_2 + 1]
            
            log.info("\n******* Inicializa Grupo {} de {} *******\n".format(idx_group, total_groups))
            log.info("{} {} {}".format(arr_rango, limit_1, limit_2))            
            
            lst_threads = []
            for equipo in arr_rango:
                id_equipo = equipo[0]
                clave_zona = equipo[1]
                if useThreads:
                    t = Process (target = fn_calcula_perfiles_horarios_por_equipo, args = (guardarEnBD, clave_zona, ianio, tipo_equipo, id_equipo, lst_meses))
                    lst_threads.append(t)
                    log.info("Genera Thread: {} ─ {} id_equipo={} PID={}".format(tipo_equipo, clave_zona, id_equipo, t.pid))
                else:
                    log.info("Genera Unique Thread: {} ─ {} id_equipo={}".format(tipo_equipo, clave_zona, id_equipo))
                    fn_calcula_perfiles_horarios_por_equipo(guardarEnBD, clave_zona, ianio, tipo_equipo, id_equipo, lst_meses)
                #endif-useThreads
        
            start_time = time.time()
            log.info("Ejecutando Grupo de Threads: {} de {}. Indices del {} al {}.".format(idx_group, total_groups, limit_1, limit_2))            
            
            if useThreads:
                [t.start() for t in lst_threads]
                [t.join(THREAD_JOIN_TIME_OUT_SEGS) for t in lst_threads]
            #endif-useThreads
            
            for t in lst_threads :
                if not t.is_alive() :
                    log.info("Eliminando Thread Not Alive: {}".format(t.pid))
                    del t
            #end-for-t
            
            elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
            log.info("******* Completado Grupo de Threads {} de {}. Indices: {} al {}. Time: {}".format(idx_group, total_groups, limit_1, limit_2, elapsed_time_fmt))
            
            idx_group = idx_group + 1
        #end while
        
        log.info("calc_perf_hr: ************** COMPLETADOS todos los bloques de Threads ************** ")        
        
        #>>> time.sleep(THREAD_MAIN_SEGS_SLEEP)
        
    except Exception as e:        
        log.error("calc_perf_hr:error>>> {}".format(str(e)), exc_info=True)
    finally:    
        if cursor:
            cursor.close()
        if connection:
            connection.close()      
        
    log.info("calc_perf_hr:main thread completed")
    return

def get_sql_base(tipo_equipo, lst_div):
    sql_base = """SELECT distinct id_catalogo_equipos, clave_zona, clave_division
                	from apcc.catalogo_equipos                        
                		where tipo_equipo = '[TIPO_EQUIPO]'
                        and clave_division in [LST_DIVISION_IN]
                        /***and id_catalogo_equipos = 3820***/
                        order by case 
                        	when clave_division = 'DA' then 1
                        	when clave_division = 'DB' then 2 
                        	when clave_division = 'DC' then 3 
                        	when clave_division = 'DD' then 4
                        	when clave_division = 'DF' then 5 		
                        	when clave_division = 'DG' then 6
                        	when clave_division = 'DJ' then 7 
                        	when clave_division = 'DK' then 8
                        	when clave_division = 'DL' then 9 
                        	when clave_division = 'DM' then 10 
                        	when clave_division = 'DN' then 11
                        	when clave_division = 'DP' then 12
                        	when clave_division = 'DU' then 13
                        	when clave_division = 'DV' then 14
                        	when clave_division = 'DW' then 15
                        	when clave_division = 'DX' then 16
                        end asc
                        LIMIT 1
						"""
    s_lst_div = "({})".format(",".join(["'{}'".format(x) for x in lst_div]))
    sql_base = sql_base.replace("[LST_DIVISION_IN]", s_lst_div)
    sql_base = sql_base.replace("[TIPO_EQUIPO]", tipo_equipo)
    sql_base = sql_base.replace("[parametros_hist_mes]", parametros_hist_mes[tipo_equipo])
    
    return sql_base
    

##########################
# main
##########################
    
def main():
    
    log.info(u"***[START]******************************************************")
    log.info("Connected to Server: {}, Database: {}".format(CN['host'], CN['database']))
    start_time = utlpy.current_time()
    start_time_date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    
    MAX_THREADS = 80 #>>100
    anio = "2018"
    tipo_equipo = "CIRCUITO" #("TRANSFORMADOR, "CIRCUITO")
    lst_divisiones = ['DA','DB','DC','DD','DF','DG','DJ','DK','DL','DM','DN','DP','DU','DV','DW','DX']
    lst_meses = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC']    
    lst_meses = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC']
    
    sql_base = get_sql_base(tipo_equipo, lst_divisiones)
    
    calc_siapcc.fn_create_if_not_exists__hist_mes_AND_perf_horarios(tipo_equipo, True)
    calc_siapcc.fn_truncate__hist_mes_AND_perf_horarios(tipo_equipo)
    
    proceso_calcula_perfiles_horarios(True, True, sql_base, MAX_THREADS, anio, tipo_equipo, lst_divisiones, lst_meses)
    
    elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
    end_time_date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    log.info(u"__main__ completed. Init: {} End: {} ─ Time: {}".format(start_time_date, end_time_date, elapsed_time_fmt))
    log.info(u"********************************************************[END]***")
    
    return  

if __name__ == '__main__' :
    main()
