# Pipeline Apache Beam / Diplomado en Data Engineer 

**Autor:** Carlos Sáez  

---

## Propósito del Pipeline

La **Helicopter Racing League (HRL)** es una liga internacional de carreras de helicópteros que transmite sus competencias con métricas de telemetría e interacción de los fans (*Fan Engagement*).  

El propósito de este pipeline es **integrar, limpiar y enriquecer** los datos de interacción provenientes de múltiples archivos **JSON**, combinándolos con información demográfica desde un archivo **CSV**, para generar un dataset final en formato **JSON Lines (.jsonl)** listo para análisis o carga a un sistema de inteligencia de negocio.

---

## 🗂️ Estructura del repositorio

```
PIPELINE_APACHE_BEAM_ENTREGA1_CS/
│
├── .devcontainer/                 # Configuración del entorno en VS Code / Docker
│   ├── Dockerfile                 # Define la imagen base y dependencias del entorno (Python, Beam, etc.)
│   └── devcontainer.json          # Configura VS Code para abrir el proyecto dentro del contenedor Docker
│
├── input/                         # Archivos JSON (fuente principal)
│   ├── cup25_fan_engagement-000-of-001.json
│   ├── league04_fan_engagement-000-of-001.json
│   └── race11_fan_engagement-000-of-001.json
│
├── input_side/                    # CSV auxiliar (enriquecimiento)
│   └── country_data_v2.csv
│
├── output/                        # Resultado del pipeline (archivo JSONL enriquecido)
│   └── sample0-00000-of-00001.jsonl
│
├── src/                           # Código fuente
│   └── pipeline.py                # Pipeline ETL con Apache Beam
│
├── requirements.txt               # Dependencias del proyecto (Beam, pandas, etc.)
└── README.md                      # Documentación e instrucciones de ejecución

```

---

## Instrucciones para su ejecución

### Ejecución en DevContainer

#### **1. Requisitos previos**
- VS Code + extensión **Dev Containers** (o GitHub Codespaces).  
- **Docker Desktop** activo.  
- Clonar el repositorio:  
  ```bash
  git clone https://github.com/carlosaezp/Pipeline_Apache_Beam_Entrega1_CS.git
  ```

#### **2. Abrir el proyecto en Docker**
Al abrir la carpeta, VS Code detectará automáticamente el entorno definido en `.devcontainer/` y mostrará el mensaje:  
> “This workspace has a Dev Container configuration. Reopen in Container?”

Seleccionar **“Reopen in Container”**.

#### **3. Ejecutar el pipeline dentro del contenedor**
```bash
python src/pipeline.py --runner DirectRunner --output_folder output --output_prefix sample0
```

---

### Ejecución en Google Colab

#### **1. Carga de archivos**

**Desde GitHub:**
```bash
!git clone https://github.com/carlosaezp/Pipeline_Apache_Beam_Entrega1_CS.git
```

**Desde carga local (opcional):**
```python
from google.colab import files
uploaded = files.upload()
!unzip -o "*.zip" -d /content/ > /dev/null && rm -f *.zip
```

#### **2. Ubicar carpeta**
```bash
%cd /content/Pipeline_Apache_Beam_Entrega1_CS
```

#### **3. Instalar dependencias**
```bash
!pip install -r requirements.txt
```

#### **4. Repetir pasos 2 y 3 según entorno**

#### **5. Ejecutar pipeline**
```bash
!python src/pipeline.py --runner DirectRunner --output_folder output --output_prefix sample0
```

---

## Lógica de transformación

El pipeline implementa un flujo **ETL (Extract – Transform – Load)** con **Apache Beam**, para limpiar, estandarizar y enriquecer los datos de interacción de los fans de la HRL.

### **1. Extracción**
- Lectura de múltiples archivos JSON con métricas de interacción.  
- Lectura de un archivo CSV con información de países.

### **2. Estandarización**
- Normalización del campo `RaceID` al formato `<string><número>` (ejemplo: *Cup 25 → cup25*).  
- Eliminación de registros donde `DeviceType` sea `"Other"`.

### **3. Enriquecimiento**
- Unión de cada registro JSON con la información del CSV según el campo `ViewerLocationCountry`.  
- Creación de una estructura anidada `LocationData` con los campos:
  - `country`  
  - `capital`  
  - `continent`  
  - `official_language`  
  - `currency`

### **4. Proyección**
- Reorganización de columnas, manteniendo solo las necesarias para el esquema final.  
- Eliminación de campos redundantes o irrelevantes.

### **5. Carga (Load)**
- Escritura del resultado en formato **JSON Lines (.jsonl)** dentro de `output/`.  
- Cada línea representa un registro completo y enriquecido, compatible con herramientas como **BigQuery**, **Athena** y **pandas**.

---

## Resultado final

El pipeline genera el archivo:

```
output/sample0-00000-of-00001.jsonl
```

El cual contiene la información enriquecida y estandarizada, lista para análisis o integración en una plataforma de visualización o Data Warehouse.
