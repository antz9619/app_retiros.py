import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import os
import logging
from datetime import datetime
import tempfile
import io
import base64

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración OCA
def get_oca_config():
    """Obtener configuración OCA de secrets"""
    try:
        # En Streamlit Cloud, usa st.secrets
        oca_usr = st.secrets.get("OCA_USR")
        oca_psw = st.secrets.get("OCA_PSW")
        
        if not oca_usr or not oca_psw:
            st.error("❌ Credenciales OCA no configuradas en secrets")
            return None
            
        return {
            "usr": oca_usr,
            "psw": oca_psw,
            "url_envios": "http://webservice.oca.com.ar/ePak_tracking/Oep_TrackEPak.asmx/IngresoORMultiplesRetiros",
            "url_centros_imposicion": "http://webservice.oca.com.ar/epak_tracking/Oep_TrackEPak.asmx/GetCentrosImposicionConServiciosByCP",
            "url_etiquetas": "http://webservice.oca.com.ar/epak_tracking/Oep_Trackepak.asmx/GetPdfDeEtiquetasPorOrdenOrNumeroEnvioParaEtiquetadora",
            "origen": {
                "nombre": "CIC",
                "apellido": "Logistica",
                "calle": "Septiembre",
                "nro": "151",
                "cp": "1625",
                "localidad": "Escobar",
                "provincia": "BUENOS AIRES",
                "email": "pedidosargentina@fuxion.net",
                "idfranjahoraria": "1",
                "centrocosto": "0",
                "nrocuenta": "191952/000"
            }
        }
    except Exception as e:
        st.error(f"Error cargando configuración: {e}")
        return None

# Cargar configuración
OCA_CONFIG = get_oca_config()

# ======================================================================
# Funciones auxiliares
# ======================================================================
def convertir_mayusculas(valor):
    return valor.strip().upper() if isinstance(valor, str) else str(valor).strip()

def obtener_centro_imposicion(cp):
    if not OCA_CONFIG:
        return "0"
        
    url = OCA_CONFIG["url_centros_imposicion"]
    payload = {"CodigoPostal": cp}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        xml_content = ET.fromstring(response.content)
        centro_id = xml_content.findtext(".//IdCentroImposicion")
        return centro_id if centro_id else "0"
    except Exception as e:
        logging.error(f"Error al obtener centro de imposición: {e}")
        return "0"

def validar_estructura(df):
    required_columns = {
        'obs': 'int64',
        'Nombre': 'object',
        'Direccion': 'object',
        'Numero': 'int64',
        'localidad': 'object',
        'provincia': 'object',
        'cp': 'int64',
        'telefono': 'object',
        'mail': 'object',
        'Referencia': 'object',
        'cantidad': 'int64'
    }
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")

    for col, dtype in required_columns.items():
        try:
            if col in ['Numero', 'obs', 'cp', 'cantidad']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                if df[col].isnull().any():
                    raise ValueError(f"Valores inválidos en {col}")
            
            elif col == 'Referencia':
                df[col] = df[col].astype(str).fillna('')
                df[col] = df[col].str.strip().str.upper()
                df[col] = df[col].replace(['NAN', 'NONE', '<NA>', 'nan'], '', regex=True)
                
            else:
                if not pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].astype(str)
                
        except Exception as e:
            raise ValueError(f"Error validando {col}: {e}")

    nombres_invalidos = df[~df['Nombre'].str.contains(',', na=False)].index
    if len(nombres_invalidos) > 0:
        lineas = [str(i + 2) for i in nombres_invalidos]
        raise ValueError(f"Nombre inválido en filas: {', '.join(lineas)}. Usar 'Apellido, Nombre'.")

def generar_xml_oca_retiros(df):
    if not OCA_CONFIG:
        raise ValueError("Configuración OCA no disponible")
        
    root = ET.Element("ROWS")
    ET.SubElement(root, "cabecera", ver="2.0", nrocuenta=OCA_CONFIG["origen"]["nrocuenta"])
    origenes = ET.SubElement(root, "origenes")

    grouped = df.groupby("obs")
    for remito, grupo in grouped:
        fila = grupo.iloc[0]
        
        origen_attrs = {
            "calle": convertir_mayusculas(fila["Direccion"]),
            "nro": str(fila["Numero"]),
            "cp": str(fila["cp"]),
            "localidad": convertir_mayusculas(fila["localidad"]),
            "provincia": convertir_mayusculas(fila["provincia"]),
            "email": fila["mail"].strip(),
            "idfranjahoraria": OCA_CONFIG["origen"]["idfranjahoraria"],
            "centrocosto": OCA_CONFIG["origen"]["centrocosto"],
            "idcentroimposicionorigen": obtener_centro_imposicion(str(fila["cp"])),
            "fecha": datetime.now().strftime("%Y%m%d"),
            "piso": "",
            "depto": "",
            "contacto": "",
            "solicitante": "",
            "observaciones": ""
        }

        origen = ET.SubElement(origenes, "origen", **origen_attrs)
        envios = ET.SubElement(origen, "envios")
        envio = ET.SubElement(envios, "envio", idoperativa="441846", nroremito=str(remito))

        destinatario_attrs = {
            "apellido": convertir_mayusculas(OCA_CONFIG["origen"].get("apellido", "")),
            "nombre": convertir_mayusculas(OCA_CONFIG["origen"].get("nombre", "")),
            "calle": convertir_mayusculas(OCA_CONFIG["origen"]["calle"]),
            "nro": OCA_CONFIG["origen"]["nro"],
            "localidad": convertir_mayusculas(OCA_CONFIG["origen"]["localidad"]),
            "provincia": convertir_mayusculas(OCA_CONFIG["origen"]["provincia"]),
            "cp": OCA_CONFIG["origen"]["cp"],
            "telefono": OCA_CONFIG["origen"].get("telefono", ""),
            "email": OCA_CONFIG["origen"].get("email", ""),
            "observaciones": "",
            "piso": "",
            "depto": "",
            "idci": "0",
            "celular": ""
        }
        ET.SubElement(envio, "destinatario", **destinatario_attrs)

        paquetes = ET.SubElement(envio, "paquetes")
        ET.SubElement(paquetes, "paquete",
                      alto="30.00", ancho="25.00", largo="20.00",
                      peso="0.20", valor="0.00", cant="1")

    return ET.tostring(root, encoding="iso-8859-1", xml_declaration=True)

def descargar_etiquetas_pdf_10x15(orden_retiro):
    """Descargar etiquetas PDF 10x15 desde OCA"""
    try:
        if not OCA_CONFIG:
            return None, "Configuración OCA no disponible"

        url = OCA_CONFIG["url_etiquetas"]
        payload = {
            "ordenRetiro": orden_retiro,
            "numeroEnvio": "",
            "logisticaInversa": "false"
        }

        response = requests.post(url, data=payload, timeout=30)
        response.raise_for_status()
        raw_xml = response.content.decode("utf-8")

        logging.info(f"XML recibido para orden {orden_retiro}")

        root = ET.fromstring(raw_xml)
        string_node = next((elem for elem in root.iter() if 'string' in elem.tag), None)

        if string_node is None or not string_node.text.strip():
            logging.error("Error: Nodo <string> no encontrado o está vacío.")
            return None, "Respuesta de OCA no contiene PDF válido"

        try:
            pdf_data = base64.b64decode(string_node.text)
        except base64.binascii.Error as e:
            logging.error(f"Error al decodificar el PDF: {e}")
            return None, f"Error al procesar el PDF recibido: {e}"

        return pdf_data, None

    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            error_content = e.response.content.decode("utf-8", errors="ignore")
            logging.error(f"Error HTTP: {str(e)}\nCuerpo de la respuesta:\n{error_content}")
            return None, f"Error de conexión con OCA: {str(e)}"
        else:
            logging.error(f"Error HTTP: {str(e)}")
            return None, f"Error de conexión con OCA: {str(e)}"
    except Exception as e:
        logging.error(f"Error inesperado: {str(e)}")
        return None, f"Error inesperado: {str(e)}"

def procesar_retiros_streamlit(archivo_subido):
    if not OCA_CONFIG:
        return {
            'exito': False,
            'error': "Configuración OCA no disponible. Verifique las credenciales en secrets.toml"
        }
        
    try:
        # Leer el archivo
        df = pd.read_excel(
            archivo_subido,
            dtype={'obs': str, 'telefono': str, 'Numero': str, 'cp': str}
        )
        
        # Normalización
        df['obs'] = df['obs'].str.strip().str.upper()
        df = df.map(lambda x: convertir_mayusculas(str(x)) if pd.notnull(x) else x)
        
        # Validar estructura
        validar_estructura(df)
        
        grouped = df.groupby('obs')
        nros_envio_total = []
        ordenes_retiro = []
        resultados_por_remito = {}

        namespaces = {
            "diffgr": "urn:schemas-microsoft-com:xml-diffgram-v1",
            "ns": "",
            "msdata": "urn:schemas-microsoft-com:xml-msdata"
        }

        # Crear directorio temporal para archivos
        with tempfile.TemporaryDirectory() as temp_dir:
            progress_bar = st.progress(0)
            total_remitos = len(grouped)
            current_remito = 0
            
            for remito, grupo in grouped:
                try:
                    current_remito += 1
                    progress_bar.progress(current_remito / total_remitos, 
                                        text=f"Procesando remito {remito} ({current_remito}/{total_remitos})")
                    
                    xml_data = generar_xml_oca_retiros(grupo)
                    xml_path = os.path.join(temp_dir, f"retiro_{remito}.xml")
                    
                    with open(xml_path, "wb") as f:
                        f.write(xml_data)

                    # Enviar a OCA
                    response = requests.post(
                        OCA_CONFIG["url_envios"],
                        data={
                            "usr": OCA_CONFIG["usr"],
                            "psw": OCA_CONFIG["psw"],
                            "XML_Datos": xml_data.decode("iso-8859-1"),
                            "ConfirmarRetiro": "True",
                            "ArchivoCliente": "",
                            "ArchivoProceso": ""
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=45
                    )
                    response.raise_for_status()

                    resultado = response.content.decode("iso-8859-1")
                    
                    # Procesar respuesta
                    root = ET.fromstring(resultado)
                    errores = root.findall(".//diffgr:diffgram/Errores/Error/Descripcion", namespaces)
                    
                    if errores:
                        error_msg = "; ".join(e.text for e in errores)
                        if "IdCodPostal" in error_msg:
                            error_msg += f" - Verifique el código postal '{grupo.iloc[0]['cp']}' para remito {remito}"
                        raise ValueError(f"Error de OCA para remito {remito}: {error_msg}")

                    detalles = root.findall(".//diffgr:diffgram/ns:Resultado/ns:DetalleIngresos", namespaces)
                    nros_envio = [
                        ''.join(filter(str.isdigit, detalle.findtext("ns:NumeroEnvio", namespaces=namespaces).strip()))
                        for detalle in detalles
                        if detalle.findtext("ns:NumeroEnvio", namespaces=namespaces)
                    ]
                    orden_retiro = detalles[0].findtext("ns:OrdenRetiro", namespaces=namespaces).strip() if detalles else None

                    if not nros_envio or not orden_retiro:
                        raise ValueError(f"No se encontraron números de envío o orden de retiro para remito {remito}")

                    orden_retiro = ''.join(filter(str.isdigit, orden_retiro))

                    nros_envio_total.extend(nros_envio)
                    ordenes_retiro.append(orden_retiro)
                    resultados_por_remito[remito] = {
                        'nros_envio': nros_envio,
                        'orden_retiro': orden_retiro,
                        'tipo': 'retiro',
                        'estado': 'éxito'
                    }
                    
                except Exception as e:
                    logging.error(f"Error procesando remito {remito}: {str(e)}", exc_info=True)
                    resultados_por_remito[remito] = {
                        'error': str(e),
                        'estado': 'error'
                    }

            progress_bar.empty()

            # Actualizar DataFrame con resultados
            for remito, resultado in resultados_por_remito.items():
                if 'nros_envio' in resultado:
                    df.loc[df['obs'] == remito, 'Nro Envío'] = resultado['nros_envio'][0]
                    df.loc[df['obs'] == remito, 'Orden Retiro'] = resultado['orden_retiro']
                    df.loc[df['obs'] == remito, 'Estado'] = 'Procesado'
                else:
                    df.loc[df['obs'] == remito, 'Estado'] = f'Error: {resultado["error"]}'

            # Guardar archivo procesado en memoria
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Retiros_Procesados')
            output.seek(0)

            return {
                'exito': len(nros_envio_total) > 0,
                'nros_envio': nros_envio_total,
                'ordenes_retiro': ordenes_retiro,
                'resultados_por_remito': resultados_por_remito,
                'archivo_procesado': output,
                'dataframe': df,
                'nombre_archivo': archivo_subido.name
            }

    except Exception as e:
        logging.error(f"Error en procesamiento: {str(e)}", exc_info=True)
        return {
            'exito': False,
            'error': str(e)
        }

def main():
    st.set_page_config(
        page_title="Sistema de Retiros OCA",
        page_icon="📦",
        layout="wide"
    )

    st.title("📦 Sistema de Retiros OCA")
    st.markdown("---")

    # Inicializar estado de la sesión
    if 'procesamiento_completado' not in st.session_state:
        st.session_state.procesamiento_completado = False
    if 'resultados_procesamiento' not in st.session_state:
        st.session_state.resultados_procesamiento = None
    if 'archivo_procesado' not in st.session_state:
        st.session_state.archivo_procesado = None

    # Verificar configuración
    if not OCA_CONFIG:
        st.error("""
        ❌ **Configuración no encontrada**
        
        Para usar esta aplicación, configure las credenciales OCA en Streamlit Secrets:
        
        - `OCA_USR`: Tu usuario OCA
        - `OCA_PSW`: Tu contraseña OCA
        
        **En Streamlit Cloud:** Ve a Settings → Secrets y agrega:
        ```toml
        OCA_USR = "tu_usuario"
        OCA_PSW = "tu_password"
        ```
        """)
        return

    # Si ya se completó un procesamiento, mostrar solo los resultados
    if st.session_state.procesamiento_completado and st.session_state.resultados_procesamiento:
        mostrar_resultados(st.session_state.resultados_procesamiento)
        return

    # Sección de subida de archivo (solo se muestra si no hay procesamiento completado)
    st.header("1. Subir Archivo de Retiros")
    
    archivo_subido = st.file_uploader(
        "Seleccione el archivo Excel con los retiros",
        type=['xlsx', 'xls'],
        help="El archivo debe contener las columnas: obs, Nombre, Direccion, Numero, localidad, provincia, cp, telefono, mail, Referencia, cantidad"
    )

    if archivo_subido:
        # Mostrar vista previa
        st.subheader("Vista Previa del Archivo")
        try:
            df_preview = pd.read_excel(archivo_subido)
            st.dataframe(df_preview.head(), use_container_width=True)
            
            # Mostrar información del archivo
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Registros", len(df_preview))
            with col2:
                st.metric("Remitos únicos", df_preview['obs'].nunique())
            with col3:
                st.metric("Columnas", len(df_preview.columns))
            
            # Volver al inicio del archivo para reprocesar
            archivo_subido.seek(0)
            
        except Exception as e:
            st.error(f"Error al leer el archivo: {e}")

    # Botón de procesamiento
    if archivo_subido and st.button("🚀 Procesar Retiros", type="primary", use_container_width=True):
        st.markdown("---")
        st.header("2. Procesando Retiros...")
        
        with st.spinner("Procesando retiros con OCA..."):
            resultado = procesar_retiros_streamlit(archivo_subido)

        # Guardar resultados en el estado de la sesión
        st.session_state.procesamiento_completado = True
        st.session_state.resultados_procesamiento = resultado
        st.session_state.archivo_procesado = archivo_subido.name

        # Mostrar resultados
        mostrar_resultados(resultado)

    # Información de ayuda
    with st.expander("📋 Estructura Requerida del Archivo"):
        st.markdown("""
        | Columna | Tipo | Descripción | Ejemplo |
        |---------|------|-------------|---------|
        | **obs** | Numérico | Número de remito | `12345` |
        | **Nombre** | Texto | "Apellido, Nombre" | `"PEREZ, JUAN"` |
        | **Direccion** | Texto | Calle | `"AVENIDA CORRIENTES"` |
        | **Numero** | Numérico | Número de dirección | `1234` |
        | **localidad** | Texto | Localidad | `"CAPITAL FEDERAL"` |
        | **provincia** | Texto | Provincia | `"BUENOS AIRES"` |
        | **cp** | Numérico | Código postal | `1001` |
        | **telefono** | Texto | Teléfono | `"1145678901"` |
        | **mail** | Texto | Email | `"cliente@email.com"` |
        | **Referencia** | Texto | Referencia opcional | `"PISO 3 DEPTO A"` |
        | **cantidad** | Numérico | Cantidad de paquetes | `1` |
        """)

    with st.expander("🔧 Configuración Actual"):
        if OCA_CONFIG:
            st.success("✅ Credenciales OCA configuradas correctamente")
            st.json({
                "nrocuenta": OCA_CONFIG["origen"]["nrocuenta"],
                "origen": OCA_CONFIG["origen"]["localidad"],
                "operativa_retiros": "441846"
            })
        else:
            st.error("Configuración no disponible")

def mostrar_resultados(resultado):
    """Función para mostrar los resultados del procesamiento"""
    st.header("📊 Resultados del Procesamiento")
    
    # Botón para procesar un nuevo archivo
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Procesar un Nuevo Archivo", type="secondary", use_container_width=True):
            # Resetear el estado de la sesión
            st.session_state.procesamiento_completado = False
            st.session_state.resultados_procesamiento = None
            st.session_state.archivo_procesado = None
            st.rerun()
    
    st.markdown("---")

    if resultado['exito']:
        st.success("✅ Procesamiento completado exitosamente!")
        
        # Mostrar información del archivo procesado
        if 'nombre_archivo' in resultado:
            st.info(f"**Archivo procesado:** {resultado['nombre_archivo']}")
        
        # Resumen general
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Números de Envío", len(resultado['nros_envio']))
        with col2:
            st.metric("Órdenes de Retiro", len(resultado['ordenes_retiro']))
        with col3:
            remitos_procesados = len([r for r in resultado['resultados_por_remito'].values() if r.get('estado') == 'éxito'])
            st.metric("Remitos Exitosos", remitos_procesados)

        # Detalles por remito
        st.subheader("Detalles por Remito")
        
        # Crear tabs para organizar los resultados
        tab1, tab2 = st.tabs(["✅ Exitosos", "❌ Con Errores"])
        
        with tab1:
            remitos_exitosos = {k: v for k, v in resultado['resultados_por_remito'].items() if v.get('estado') == 'éxito'}
            if remitos_exitosos:
                for remito, detalle in remitos_exitosos.items():
                    with st.expander(f"📦 Remito: {remito} - OR: {detalle['orden_retiro']}"):
                        st.write(f"**Números de envío:** {', '.join(detalle['nros_envio'])}")
                        st.write(f"**Orden de retiro:** {detalle['orden_retiro']}")
                        
                        # Botón para descargar etiquetas PDF
                        st.subheader("🎫 Etiquetas PDF")
                        if st.button(f"Descargar Etiquetas PDF", key=f"pdf_{detalle['orden_retiro']}"):
                            with st.spinner("Generando etiquetas PDF..."):
                                pdf_data, error = descargar_etiquetas_pdf_10x15(detalle['orden_retiro'])
                            
                            if pdf_data:
                                st.success("✅ Etiquetas generadas correctamente")
                                st.download_button(
                                    label="📄 Descargar Etiquetas PDF 10x15",
                                    data=pdf_data,
                                    file_name=f"etiquetas_{detalle['orden_retiro']}_10x15.pdf",
                                    mime="application/pdf",
                                    key=f"download_pdf_{detalle['orden_retiro']}"
                                )
                            else:
                                st.error(f"❌ Error al generar etiquetas: {error}")
            else:
                st.info("No hay remitos exitosos")
        
        with tab2:
            remitos_error = {k: v for k, v in resultado['resultados_por_remito'].items() if v.get('estado') == 'error'}
            if remitos_error:
                for remito, detalle in remitos_error.items():
                    with st.expander(f"❌ Remito: {remito}"):
                        st.error(f"**Error:** {detalle['error']}")
            else:
                st.info("No hay remitos con errores")

        # Descargar archivo procesado
        st.subheader("📥 Descargar Resultados")
        st.download_button(
            label="Descargar Archivo Procesado",
            data=resultado['archivo_procesado'],
            file_name=f"retiros_procesados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.ms-excel",
            use_container_width=True
        )

    else:
        st.error("❌ Hubo errores en el procesamiento")
        if 'error' in resultado:
            st.error(f"**Error:** {resultado['error']}")

if __name__ == "__main__":
    main()