package com.ipartek.formacion.migracion;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class App {

	// variables globales
	//static final String PATH_TXT_PERSONAS = "data/personas20.txt";
	static final String PATH_TXT_PERSONAS = "data/personas.txt";
	static int cntInsercciones = 0;
	static int cntErrores = 0;
	static String msjErrores = ""; // Guardar mensajes de lineas erroneas
									// separadas por \n

	// conexion bbdd
	static Connection conn;
	static PreparedStatement pst;
	static final String CONN_URL = "jdbc:mysql://localhost/iparsex";
	static final String CONN_USER = "root";
	static final String CONN_PASS = "";
	
	

	// medicion de tiempos de ejecucion
	static long tInicio;
	static long tFin;

	public static void main(String[] args) {
		System.out.println("Comenzando migracion......");

		tInicio = System.currentTimeMillis();


		try {	
			// Abrir fichero personas
			FileReader f = new FileReader(PATH_TXT_PERSONAS);
			BufferedReader br = new BufferedReader(f);
			
			//abrir base datos
			abrirBaseDatos();
			
			String linea;
			int i = 1;
			String [] aCampos;			
			// leer linea a linea
			while ((linea = br.readLine()) != null) { // while loop begins here
		         System.out.println( i + " - " + linea);
		         try{			         
			         aCampos = linea.split(",");
			         
   			        //comprobar que linea tenga datos correctos
			       if ( aCampos.length == 7 && insertarPersona(aCampos) ){  
			    	   
			    	   //sumar contador
			    	   cntInsercciones++;   
			    	   
			       }else{
				    	// FALSE: msjErrores add linea y sumar contador errores
				    	msjErrores += linea + " \n";   
				    	cntErrores++;   
			       }	         
			       
		         }catch(Exception e){
		        	 System.out.println("Excepcion leyendo fila " + i);
		        	 e.printStackTrace();		        	 
		         }    
		         i++;
		         
			} // end while 
			
			// Cerrar fichero personas
			if (br != null) {
				br.close();
			}
			if (f!=null){
				f.close();
			}	

		} catch ( SQLException e){
			System.out.println("Excepcion con Base Datos");
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			System.out.println("Fichero no encontrado " + PATH_TXT_PERSONAS);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			cerrarBaseDatos();
		}
		

		
		// resumen del proceso
		tFin = System.currentTimeMillis();
		resumen();

	}
	
	/**
	 * Inserta en la BBDD la persona representada por una linea del fichero de texto
	 * @param aCampos {@code array String} contiene 7 campos para la persona <br>
	 * <ul>
	 * 	<li>[0] Nombre</li>
	 *  <li>[1] Apellido1</li>
	 *  <li>[2] Apellido2</li>
	 *  <li>[3] Edad</li>
	 *  <li>[4] Email</li>
	 *  <li>[5] Dni</li>
	 *  <li>[6] Rol</li>
	 * </ul>
	 */
	private static boolean insertarPersona(String [] aCampos) {
		
		boolean resul = false;
		try{
			String insertSql = "INSERT INTO `persona` (`pass`, `nombre`, `dni`, `observaciones`) VALUES ( ? , ? , ? , ?);";
			pst = conn.prepareStatement(insertSql);
			pst.setString(1, "1111");
			pst.setString(2, aCampos[0] + " " + aCampos[1] + " " + aCampos[2]);
			pst.setString(3, aCampos[5] );
			pst.setString(4, aCampos[6] );
			
			if ( pst.executeUpdate() == 1 ){			
				resul = true;
			}	
			
		}catch( Exception e){
			e.printStackTrace();			
		}	
		
		return resul;
	}

	private static void abrirBaseDatos() throws SQLException {
		
		conn = DriverManager.getConnection ( CONN_URL, CONN_USER, CONN_PASS );
		System.out.println( "Base Datos Abierta");
		
	}

	private static void cerrarBaseDatos()  {
		
		try{
			if ( pst != null ){
				pst.close();
			}
			
			if ( conn != null ){
				conn.close();
			}
			
			System.out.println("Base Datos cerrada");
			
		}catch( Exception e){
			
			System.out.println("Excepcion cerrando recursos BBDD");
			e.printStackTrace();
		}	
		
	}

	

	private static void resumen() {

		System.out.println("---------------------------------------------------------------------");
		System.out.println("Proceso terminado: " + ((tFin - tInicio) / 1000) + " segundos");
		System.out.println("Lineas insertadas: " + cntInsercciones);
		System.out.println("Lineas erroneas: " + cntErrores);
		System.out.println("Detalle Lineas erroneas: ");
		System.out.println(msjErrores);

	}

}
