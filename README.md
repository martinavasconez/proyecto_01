# Data-Mining / Proyecto 01 

## Descripción 
En este proyecto se implementaron tres pipelines de ETL (Extract, Transform, Load) para integrar datos de Quickbooks Online (QBO) en un esquema "raw" de PostgreSQL, usando Mage.ai como orquestador.

Se construyeron **tres pipelines independientes**, uno para cada entidad solicitada: 
- `qb_invoices_backfill` → Facturas  
- `qb_customers_backfill` → Clientes  
- `qb_items_backfill` → Ítems

Cada pipeline sigue el mismo flujo:  
- **Extract**: se establece la conexión a QBO con el protocolo OAuth2, manejamos paginación y segmentación en chunks de fechas.
- **Transform**: conversión a DataFrame.
- **Load**: carga en PostgreSQL aplicando upsert para evitar duplicados, asegurando idempotencia. Aquí también creamos una tabla de reporte de backfill para cada entidad.

## Diagrama de arquitecura
[Ver diagrama de arquitecura](https://viewer.diagrams.net/index.html?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1&title=arquitectura%20(1).drawio&dark=auto#R%3Cmxfile%3E%3Cdiagram%20id%3D%22arquitectura1%22%20name%3D%22Arquitectura%22%3E7Vxrc9q4Gv41zCQf4rEty8YfIZezncmZ023a6e75wghbgIqxWFkQ0l%2B%2Fki1jWxIJWyCXTZi02K8ulvU8evVelPTA5WLzH4aWs%2F%2FSFGc93003PXDV833fA6H4kpKHWuJGlWTKSFrJvEZwR35iJXSVdEVSXHQqckozTpZdYULzHCe8I0OM0ftutQnNuk9doik2BHcJykzpd5LyWSXt%2B1Ej%2Fw2T6ax%2BshfGVckC1ZXVmxQzlNL7lghc98Alo5RXV4vNJc7k7NXzUrW72VG6HRjDOd%2Bnweh2GP1%2FOv%2Fj2z0YLi9%2B3K1vv7MLz6%2B6WaNspd5YjZY%2F1FOAimU1sxOywaKzYTHHPJEv54qbGV9k4tITlykqZrJCKb%2BfEY6LJUpkL%2FeCHUK2xowTMbe3aIyzz7QgnNBcFI8p53TRqjDIyFQWcCqbTUiWXdKMsnI8wO9H8NqX4%2BCMznGrZFJ%2BRMmSkpyXyMOh%2BHEd14U9%2F9J1QiC%2FelDUviwLvFLsa9LYKi270GvGOzqOytai3NKJp8n8vlZX%2FIjXm6GlnLzFZioXlzNfjTHLsZhVhwi6yxloROWUKhyW7FNSzitaEgWvmFe82Ukcb0tHsZAxXWDOHkQV1eCirxis1jBwXSdQNLlv1oTn9Z1AkX%2FWWRLAiaBakWo1TrcPaQgrLhRn%2Fwl%2FA4O%2Fv69IMh9TOi%2BEfPD5k8FnMRO8S1yk6JaIGcHMwsMFSVPZfMhwQX6icdmVaxBNooZWXBC7VGOewdGc5lgjdC2iOb9TY%2FSOAVrUBc2PdoDmmogB90RowdgAA6dC3apbyviMTmmOsutGOmR0ladbtdLUuaVSOZST%2FANz%2FqD2Djn%2FXXQNNXFzE0RevC2p9ToQkmp8clCPz7t4B7piCX6MmtAOEMMZ4mTdfYBtqlXTz5JiDbCBGzhxCN3648EOzp6rgccRm2KuOtHw247qgAUIn95AOvsFXfGM5AKOer92FfvbGh74N3IMQ6H4UoKbMsv6EdWjwUDsu7sWW2dX%2BsUdyFAQLXqVK1cteF9byfKRxvZZ6gzMrte4Uh2eRdej%2ByJwCOUjJJj%2BIEZUjJZkieXMbVl6kH7wNaXuBa6hHPwgdmJg0eggcGD%2FVBo9fJpQZFFabebUtmCxaGYdRB051e0VWUzF0DMyFv%2BjnyuG5SuliKMxKoQt6t8MpHB0pSQjwSM%2BZfju99vRHWYCFqdYT48DU%2BiF282z1uRxZCAVhQ604OT6pwIpNkBSkyA33bMvg%2B%2Fn73PbDT1t2w1NsJ53z63djxZUf41HJF9TkuBiNEbJXM7L%2B4QLAKHhQrf5BF30hKXrx61iiwEFnhVMzwZmsirE5oWZWHthJkFLyVpcTuVlC%2Bu6VCqzpsKONhwv9m9Qk2jE8FIYaOaAnuxhj07%2F8WvUIuGE5h1uh3%2BtaP2EaWleXiQVzwYyvMBQXtQUGpahhW1ZJjG%2BSBGbn7Hp%2BEw6g0Jaf51X37LEh7C6aV%2Bcn1ejUo%2Ff%2BaLa3Ffj776T%2FeX3npIxe6rp3togwxP%2BIrrgCMs%2Fci2erRc3n9Bc76FlvW81%2BvEXPDAW%2FPVGMDSRHsHZEk1JjhLSuwS9IcjLFxF8na1y6fxe2gmAFtKizceF%2FFqgHP%2BgMkCHS9dEWqa5gKNq3am6N8kYngjQZ2Wkbo7lqP43WPGZf34w106%2B8whnIg3HIQxNJ2MymfhJcoI9yHOBE7c%2FUZeTscnBvoWD4ckoGH14eG%2FSw4viMqLYcR1eiZPn9x%2B1Y969VRo4MWyZnV0HEEBplZpaIXhWS9T0AD%2B0wlvQClrcJ3gtcR9g91OlLfze1UEQPOakQumk9h83Wv3nVA3Af1o14DwdyOysRCpDRUGSLoxN0N%2BzmGL9BJem2O7Ivui%2FvTZVEBwzIl4Rs7ulMJrzqeo9WbH1VlEUomq9rH2pW%2FCG8D9koRNXPYv7P8t74Ebq%2FmqjWpc3D62b7SOVrHfE%2FIJvZ00Ld2iBvZYdmIbwoOY6%2BRqdqvEbeQejI19PVOm83JHAEPRBD61qavnuHPDWQNae0zC96vGo2RFgpiff8FL495BX40JQq9RDyQt0L%2BzE2TewR%2Fbt7fCrpWpj2FG1IXjHmjbWyBoeiazwmVPFwJLZg8MqNiTE8lv0%2B1XFjL6oKBK82mX1lSeM7ronjLq2l%2BFgbEnLKEfKBbjwocWaP4LdBr1uXEfzxYUXVxvTbVMtflZTzfTDXwMk7mkACWVApA89EMYBjAUCXd0NgOO6AEAvjNzYC%2BvQ68uBY3GxXwE44FTo9J0A7EQn8ALHi1rowBdGp%2FabW%2FN88EklMVPsQe6AF%2FLEYlRLlLsRhbWg2QXLu4f2nb4P7kL0RU9F%2BW611zxWMbSz5cDdFGp%2Bi69vgjt202NtgkFwQtoI1giN3mFN6IJfYc0LsaPONZyeHYetfXjite9paz9qBG8BxfhNoAjNONVXmfufULbYlfxs5S3PeF0ZydGJf%2FJA2IShBf7IdNoDmb7Ma3TUL7TkMWwZ9uBU2zg0E%2By3FKVCcvZtWYi3rU5vfErxYkk5zhOCHsmNt7Pg6F6CnczwomrhNlFstzrp8cESq9uk7dA2itgS4KejiBnE8wyQRD9E8GWHmd3GTk8LaX6PnvqSaTEfwKC%2FKwNRN7giTHRcdYRRwU3I4DgC%2FXFPP1Kv56Vqq%2FEGLUgmQfgNZ2ss%2BaUZ%2FqHNvJSfXnnoSsZ9vpa7X9AIhirh1pbdlkeI2pIvCtbAStljMy7qeuaWExe2%2FIke8D4e38ygniWq98G3N8s3oB87e2nCmYE58EG4fxHh4GsjnHmk7BbJ3xzg1S8Q1Ece5HUhDyYmVFhfeRnnSnF5FPf12Ej7HgToUMtvUhSHHecIuocBLsywGLCdCzqdsdQ3sPn4BT4tnxN2IIufNwsTnjJu6ToBCLqhi9jdRqTeQOhi7%2FBk5TcenRyg37VFvX7U7WLfZB8AXZXv6cr8l2kmbps%2FWlBVb%2F72A7j%2BGw%3D%3D%3C%2Fdiagram%3E%3C%2Fmxfile%3E#%7B%22pageId%22%3A%22arquitectura1%22%7D)


## Pasos para levantar contenedores y configuar el proyecto

1. Se creó una carpeta llamada **`proyecto-01`** con el archivo `docker-compose.yaml`, donde se definen las tres imágenes y sus volúmenes.  
2. Se levantaron los servicios de **Mage** y **PostgreSQL** con Docker usando el comando:  
   ```bash
   docker compose up -d
3. Se accedió a Mage y a PostgreSQL a través de Docker Desktop, en la ventana de Containers.
4. Se configuraron los secrets en Mage, guardando las credenciales de QuickBooks y PostgreSQL.

## Gestión de Secretos
| Secreto            | Propósito                                | Rotación / Responsable |
| ------------------ | ---------------------------------------- | ---------------------- |
| `qb_client_id`     | Client ID para acceder a la API de QuickBooks |   |
| `qb_secret_id` | Secret ID para acceder a la API de QuickBooks        |     |
| `qb_refresh_token` | Refresh Token OAuth2  | Expira en 100 días    |
| `qb_access_token` | Access Token OAuth2  | Expira en 1 hora    |
| `qb_realm_id`      | Identificador de la compañía en QBO      |     |
| `pg_host`          | Host de PostgreSQL                       |                     |
| `pg_port`          | Puerto                            |                     |
| `pg_db`            | Nombre de la base de datos               |                     |
| `pg_user`          | Usuario de conexión                      |                     |
| `pg_password`      | Clave del usuario                   |                     |

## Detalle de los tres pipelines 

Cada pipeline está diseñado para extraer, transformar y cargar datos desde **QuickBooks Online (QBO)** hacia **PostgreSQL**.  
Las tres entidades soportadas son:

- `qb_invoices_backfill`
- `qb_customers_backfill`
- `qb_items_backfill`

Se va a generalizar el detalle de las 3 tuberías, básicamente la única modificación entre las 3 es la query que se hace a la API para que nos traiga la información de cada entidad.

### Parámetros principales
Cada pipeline recibe parámetros vía **Mage**:
- `start_date` → fecha inicial del backfill (`YYYY-MM-DD`)
- `end_date` → fecha final del backfill (`YYYY-MM-DD`)
- `chunk_size_days` → tamaño de cada ventana de extracción en días (default: `7`)
- `page_size` → número de registros devueltos por página desde QBO (default: `100`)
- `execution_date` → timestamp de ejecución, usado para `ingested_at_utc`

### Segmentación
- **Por tiempo:** los datos se extraen en **chunks dinámicos de fechas** (ej. semanal).
- **Por paginación:** dentro de cada chunk, QBO devuelve resultados en páginas (`STARTPOSITION` y `MAXRESULTS`).  
  - Se incrementa la posición hasta agotar los registros.  
  - El campo `page_number` se guarda en la base de datos como metadato.

### Límites
- **API QuickBooks:** Se usa `page_size=100` para evitar timeouts.
- El código implementa **sleep exponencial** (`2^n segundos`) en caso de error de conexión o `429 Too Many Requests`.
- **DB inserts:** se ejecuta un `UPSERT` fila por fila en PostgreSQL con `ON CONFLICT (id) DO UPDATE`.

### Reintentos
- Cada request a QBO tiene hasta **5 reintentos** configurados.  
- Estrategia: **exponencial backoff** → espera `2s, 4s, 8s, ...` entre intentos.  
- Si expira el `access_token` (`401 Unauthorized`), se refresca automáticamente vía `refresh_token`.

### Runbook (procedimiento de operación)

1. **Ejecución del pipeline en Mage**  
   - Ejecutar el pipeline manualmente desde Mage o vía trigger.  
   - Se pueden modificar los parámetros `start_date` y `end_date` para definir el rango a procesar.  

2. **Monitoreo del bloque *loader***  
   - Validar que los *chunks* y páginas se impriman secuencialmente.  
   - Confirmar la cantidad de filas devueltas por cada request y que no haya saltos en las fechas.  

3. **Verificación del bloque *transformer***  
   - Confirmar que los datos se convierten correctamente a `DataFrame`.  
   - Revisar en la esquina inferior izquierda el total de filas procesadas.  

4. **Confirmación del bloque *exporter***  
   - Revisar en los logs el número de filas `inserted`, `updated` y `skipped`.  
   - En caso de error, se registra un reporte en la tabla `backfill_report_<entidad>`.  

5. **Reanudación desde el último tramo exitoso**  
   - Identificar en `backfill_report_<entidad>` el último `window_start` y `window_end` que terminaron en estado `success`.  
   - Relanzar el pipeline configurando `start_date` = día siguiente del último `window_end`.  

6. **Reintentos selectivos**  
   - Si un tramo aparece con `status = error` en el reporte, volver a ejecutar el pipeline únicamente para ese rango de fechas (`start_date`, `end_date`).  
   - El proceso es idempotente, por lo que no generará duplicados.  

7. **Verificación en base de datos**  
   - Consultar en `raw.qb_<entidad>` el conteo de filas por ventana de extracción para validar la volumetría.
   - Verificar que las fechas cubren todo el rango y que los conteos son consistentes.  

## Trigger one-time: 
- **Fecha/hora en UTC configurada**: `2025-09-11T05:06:00Z`  
- **Equivalencia en Guayaquil (UTC-5)**: `2025-09-11T00:06:00-05:00`

### Política de deshabilitación
- El trigger está definido como **one-time** (ejecución única).  
- Evidencia: en Mage aparece el trigger con estado `enabled` y fecha/hora de ejecución configurada.

Se pueden configurar las variables de start_date y end_date para traer datos de distintos rangos de fechas.

## Esquema raw
Cada entidad tiene su propia tabla y comparten un mismo diseño:

Tienen las siguientes columnas:
- id (primary key)
- payload JSONB
- ingested_at_utc
- extract_window_start_utc 
- extract_window_end_utc
- page_number 
- page_size 
- request_payload

Se implementó también 3 reportes automáticos del backfill que incluye resumen de las métricas.

## Validaciones/volumetría: 

Se implementaron validaciones automáticas en el código:
- El bloque 'loader' imprime información detallada de la extracción en cada iteración:

Nuevo access_token obtenido vía refresh_token
Chunk 1: 2025-07-01T00:00:00Z → 2025-07-07T23:59:59Z
Request API: 2025-07-01T00:00:00Z → 2025-07-07T23:59:59Z, posición=1, intento 1
Posición 1 → 0 items
Chunk 1 devolvió 0 items

Esto permite llevar un registro de que está pasando mientras se cargan los datos de la API. Con esto podemos comprobar que la paginación está funcionando correctamente, no hay saltos de fechas entre chunks, cada request devuelve el número esperando de facturas, clientes o items.

Adicionalmente, para comprobar que los datos se extraen correctamente, hice la misma ejecución con la misma query desde postman y verifiqué que los datos que vienen es la misma en ambos casos.

-En el bloque `transformer` retorno la tabla ya convertida a dataframe y comparo con el resultado del bloque 'loader' para verificar que no haya perdida de datos.

-En el bloque `exporter` se imprimen logs al insertar datos en la base de datos:
Exportando 29 filas a raw.qb_customers en warehouse:5432/qbo_raw
Exportación completada: 29 filas procesadas.

Además, se genera un registro en la tabla de reportes (`backfill_report_<entidad>`):
- `row_count`: número de filas procesadas.  
- `status`: `success` o `error`.  
- `error_message`: detalle en caso de fallo.  

Esto valida que:
- El **UPSERT** asegura idempotencia (si corres 2 veces el mismo rango, no se duplican registros).  
- Cada corrida deja trazabilidad en la tabla de reportes.

De igual manera, se valida que la tabla que nos da el transformer sea la misma que se carga en la base de datos y haya la misma cantidad de columnas y filas.

Con los tres bloques se verifica las filas devueltas por chunk y pagina, cuantas filas fueron exportadas, y si la tabla fue subida correctamente. Adicional a esto, se pueden ejecutar consultas en SQL para validar volumetría.


## Troubleshooting

### Autenticación 
- **Error 401 Unauthorized**: este error indica que el 'access_token' ha expirado. El pipeline se hizo de forma robusta e identifica cuando surge este error para refrescar el token automáticamente usando el 'refresh token'.
- También ha surgido el error 400, en esas ocasiones se actualiza el 'refresh_token' generando uno nuevo.

### Paginación
- QuickBooks limita las consultas a **100 registros por request** (`MAXRESULTS`).  
- Los pipelines usan `STARTPOSITION` para iterar en páginas sucesivas.
- En caso de que se repitan o falten registros, sería necesario revisar los campos 'posicion' o 'page_number' en el payload guardado.

### Límites (Rate Limits)
- QuickBooks aplica **límite de requests por minuto**. En caso de recibir  **429 Too Many Requests**, el pipeline espera con backoff exponencial (`2^n` segundos) y reintenta.  
- Ajustar `max_retries` en el loader si el volumen de datos es muy alto, en este caso se establecieron 5 retries.

### Timezones
- QuickBooks almacena fechas en **UTC**.  
- En el pipeline se formatea `YYYY-MM-DDTHH:MM:SSZ` para asegurar compatibilidad.  
- PostgreSQL también guarda `TIMESTAMP` en UTC.  

### Almacenamiento
- Las tablas generadas ('qb_customers', 'qb_invoices', 'qb_items') almacenan los siguientes datos: el id de la entidad, payload como JSONB (dato crudo), metadatos (ingested_at_utc, ventanas de extracción, paginación).  

### Permisos
- Mage requiere accesos de escritura en la base PostgreSQL para poder insertar las tablas. Es necesario verificar que el usuario tiene priviligeos para crear el schema, crear la tabla, y hacer upserts.

## Checklist de aceptación

- [x] Mage y Postgres se comunican por nombre de servicio.  
- [x] Todos los secretos (QBO y Postgres) están en Mage Secrets; no hay secretos en el repo/entorno expuesto.  
- [x] Pipelines `qb_<entidad>_backfill` aceptan `fecha_inicio` y `fecha_fin` (UTC) y segmentan el rango.  
- [x] Trigger one-time configurado, ejecutado y luego deshabilitado/marcado como completado.  
- [x] Esquema `raw` con tablas por entidad, payload completo y metadatos obligatorios.  
- [x] Idempotencia verificada: reejecución de un tramo no genera duplicados.  
- [x] Paginación y rate limits manejados y documentados.  
- [x] Volumetría y validaciones mínimas registradas y archivadas como evidencia.  
- [x] Runbook de reanudación y reintentos disponible y seguido.  




