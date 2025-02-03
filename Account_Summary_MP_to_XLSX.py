import os
import re
import fitz  # PyMuPDF
import pandas as pd
from io import BytesIO
import concurrent.futures
import gc
import multiprocessing
import tempfile

import streamlit as st

# ---------------------------
# Funciones de procesamiento
# ---------------------------

# Expresiones regulares optimizadas
date_pattern = re.compile(r'^\d{2}-\d{2}-\d{4}')  # Fecha en formato DD-MM-YYYY
id_pattern = re.compile(r'\d+')  # ID de la operación (números)
money_pattern = re.compile(r'\$')  # Detecta el símbolo "$"
client_name_pattern = re.compile(r'^[A-Za-z\s\.,\-]{4,}')  # Nombre del cliente: mínimo 4 caracteres

# Función para convertir DataFrame a Excel
def convert_df_to_excel(df):
    df = format_dataframe(df)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resumen')
    return excel_buffer.getvalue()

# Función para sanitizar nombres de archivo
def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', '', filename).replace(' ', '_')

# Función para calcular el periodo a partir de un DataFrame
def calculate_period(df):
    min_date = pd.to_datetime(df['Fecha'], format='%d-%m-%Y').min().strftime('%d-%m-%Y')
    max_date = pd.to_datetime(df['Fecha'], format='%d-%m-%Y').max().strftime('%d-%m-%Y')
    return f"Del_{min_date}_al_{max_date}"

# Función para convertir texto del PDF a DataFrame
def pdf_text_to_dataframe(text):
    combined_lines = combine_broken_lines(text)
    data = []
    for line in combined_lines:
        fields = extract_fields_from_line(line)
        if fields:
            data.append(fields)
    df = pd.DataFrame(data, columns=['Fecha', 'Descripción', 'ID de la Operación', 'Valor', 'Saldo'])
    return df

# Función para formatear DataFrame
def format_dataframe(df):
    df['ID de la Operación'] = pd.to_numeric(df['ID de la Operación'], errors='coerce').astype('Int64')
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
    df['Saldo'] = pd.to_numeric(df['Saldo'], errors='coerce')
    return df

# Función para combinar líneas rotas
def combine_broken_lines(text):
    lines = text.split('\n')
    combined_lines = []
    current_line = ""
    for line in lines:
        if date_pattern.match(line):
            if current_line:
                combined_lines.append(current_line.strip())
            current_line = line
        else:
            current_line += " " + line
    if current_line:
        combined_lines.append(current_line.strip())
    return combined_lines

# Función para extraer campos desde una línea
def extract_fields_from_line(line):
    date_match = date_pattern.search(line)
    if not date_match:
        return None
    date = date_match.group(0)
    rest_of_line = line[date_match.end():].strip()
    money_matches = list(money_pattern.finditer(rest_of_line))
    if len(money_matches) < 2:
        return None
    first_money = money_matches[0].start()
    second_money = money_matches[1].start()
    before_money = rest_of_line[:first_money].strip()
    value_str = rest_of_line[first_money:second_money].strip()
    balance_str = rest_of_line[second_money:].strip()

    id_matches = id_pattern.findall(before_money)
    if not id_matches:
        return None
    operation_id = id_matches[-1]
    description = before_money[:before_money.rfind(operation_id)].strip()

    value = convert_money_to_number(value_str)
    balance = convert_money_to_number(balance_str)
    balance = clean_balance(balance)

    return [date, description, operation_id, value, balance]

# Función para convertir monto a número
def convert_money_to_number(money_str):
    return money_str.replace('$', '').replace('.', '').replace(',', '.').strip()

# Función para limpiar saldo
def clean_balance(balance_str):
    if '.' in balance_str:
        decimal_pos = balance_str.index('.')
        if len(balance_str) > decimal_pos + 3:
            balance_str = balance_str[:decimal_pos + 3]
    return balance_str

# Función para leer el texto del PDF usando PyMuPDF
def read_pdf_text(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            page_text = page.get_text()
            if page_text:
                text += page_text + '\n'
        doc.close()
        return text
    except Exception as e:
        print(f"No se pudo leer el archivo {pdf_path}: {e}")
        return None

# Función para procesar un solo archivo (CPU-bound)
def process_single_file(args):
    file, text = args
    df = pdf_text_to_dataframe(text)
    gc.collect()
    return df

# ---------------------------
# Interfaz en Streamlit
# ---------------------------

def process_uploaded_files(uploaded_files):
    """
    Procesa los archivos PDF subidos, genera un DataFrame consolidado con toda la información,
    y retorna un diccionario con el archivo Excel consolidado.
    """
    if not uploaded_files:
        st.warning("No se detectaron archivos PDF subidos.")
        return {}

    st.info("Guardando los archivos PDF temporalmente y extrayendo su contenido...")
    texts = []
    total_lines = 0
    temp_files = []  # Registro de archivos temporales para su eliminación

    # Guardar archivos temporales y leerlos (I/O-bound)
    for uploaded_file in uploaded_files:
        try:
            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            temp.write(uploaded_file.getvalue())
            temp.close()
            temp_files.append(temp.name)
            text = read_pdf_text(temp.name)
            if text:
                total_lines += len(text.split('\n'))
                texts.append((temp.name, text))
        except Exception as e:
            st.error(f"Error al procesar {uploaded_file.name}: {e}")

    if total_lines == 0:
        st.warning("No se encontró contenido en los archivos PDF subidos.")
        for f in temp_files:
            try:
                os.unlink(f)
            except Exception:
                pass
        return {}

    st.info("Procesando el contenido y generando DataFrames...")
    progress_text = st.empty()
    progress_bar = st.progress(0)

    # Procesar textos en paralelo (CPU-bound)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dfs = list(executor.map(process_single_file, texts))

    progress_bar.progress(100)
    progress_text.text("Archivos procesados. Preparando el archivo consolidado...")

    # Concatenar todos los DataFrames
    consolidated_df = pd.concat(dfs, ignore_index=True)
    period = calculate_period(consolidated_df)
    sanitized_period = sanitize_filename(period)
    consolidated_filename = f"CONSOLIDADO_MERCADOPAGO_{sanitized_period}.xlsx"
    consolidated_excel = convert_df_to_excel(consolidated_df)

    # Eliminar archivos temporales
    for f in temp_files:
        try:
            os.unlink(f)
        except Exception:
            pass

    st.success("Conversión completada con éxito.")
    return {"CONSOLIDADO": {'excel_data': consolidated_excel, 'output_excel_name': consolidated_filename}}

def main():
    st.title("Conversor de Resumen de Cuenta de Mercado Pago a Excel")
    st.write("Sube uno o varios archivos PDF del resumen de cuenta de Mercado Pago para generar un archivo Excel consolidado.")

    uploaded_files = st.file_uploader("Selecciona tus archivos PDF", type="pdf", accept_multiple_files=True)

    if st.button("Convertir Archivos"):
        if not uploaded_files:
            st.warning("Por favor, sube al menos un archivo PDF.")
        else:
            with st.spinner("Convirtiendo archivos, por favor espere..."):
                processed_files = process_uploaded_files(uploaded_files)
            if processed_files:
                st.write("### Descarga tu archivo Excel consolidado:")
                file_info = processed_files["CONSOLIDADO"]
                st.download_button(
                    label=f"Descargar {file_info['output_excel_name']}",
                    data=file_info['excel_data'],
                    file_name=file_info['output_excel_name'],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

if __name__ == "__main__":
    multiprocessing.freeze_support()  # Compatibilidad para Windows
    main()
