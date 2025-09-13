import time
import pandas as pd
from datetime import datetime, timedelta
import requests
import base64
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def _refresh_access_token():
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_secret_id')  
    refresh_token = get_secret_value('qb_refresh_token')

    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    creds = f"{client_id}:{client_secret}"
    encoded = base64.b64encode(creds.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded}",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    resp = requests.post(url, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    tokens = resp.json()
    print("Nuevo access_token obtenido vía refresh_token")
    return tokens["access_token"]


def _fetch_qb_data(realm_id, access_token, base_url, minor_version,
                   fecha_inicio, fecha_fin, max_retries=5):
    if not realm_id or not access_token:
        raise ValueError("Se requieren realm_id y access_token")
    if not base_url or not minor_version:
        raise ValueError("Se requiere base_url y minor_version")
    if not fecha_inicio or not fecha_fin:
        raise ValueError("Se requieren fecha_inicio y fecha_fin")

    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"
    items = []
    posicion = 1  
    tamanio_pagina=100
    page_number = 1

    while True:
        query = (
            "SELECT * FROM Item "
            f"WHERE Metadata.LastUpdatedTime >= '{fecha_inicio}' "
            f"AND Metadata.LastUpdatedTime <= '{fecha_fin}' "
            f"STARTPOSITION {posicion} "
            f"MAXRESULTS {tamanio_pagina}"
        )

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }
        params = {
            "minorversion": minor_version
            }

        intentos = 0
        while intentos < max_retries:
            try:
                print(f"Request API: {fecha_inicio} → {fecha_fin}, posición={posicion}, intento {intentos+1}")
                resp = requests.get(url, headers=headers, params={**params, "query": query}, timeout=60)

                if resp.status_code == 401:  # token expirado
                    print("Access token expirado, refrescando...")
                    access_token = _refresh_access_token()
                    headers["Authorization"] = f"Bearer {access_token}"
                    intentos += 1
                    continue

                resp.raise_for_status()
                data = resp.json()
                lote = data.get("QueryResponse", {}).get("Item", [])

                print(f"Posición {posicion} → {len(lote)} items")
                items.extend(lote)

                request_payload = {
                    "url": url,
                    "headers": {k: v for k, v in headers.items() if k != "Authorization"},
                    "params": params,
                    "query": query,
                }


                if len(lote) < tamanio_pagina:
                    return items, query, tamanio_pagina, request_payload, posicion
                else:
                    posicion += tamanio_pagina  # avanzar a la siguiente página
                    page_number +=1
                    break

            except requests.exceptions.RequestException as e:
                intentos += 1
                espera = 2 ** intentos
                print(f"Error en request ({e}), reintento {intentos}/{max_retries} en {espera}s...")
                time.sleep(espera)
        else:
            raise Exception(f"Falló la request después de {max_retries} intentos")


@data_loader
def load_data_from_api(*args, **kwargs):
    realm_id = get_secret_value('qb_realm_id')
    access_token = _refresh_access_token() #siempre refrescamos
    minor_version = 75
    base_url = "https://sandbox-quickbooks.api.intuit.com"

    start_date_str = kwargs.get("start_date")
    end_date_str = kwargs.get("end_date")
    fecha_inicio = datetime.strptime(start_date_str, "%Y-%m-%d")
    fecha_fin = datetime.strptime(end_date_str, "%Y-%m-%d")

    todos_items = []
    chunk = 1
    cursor = fecha_inicio

    while cursor <= fecha_fin:
        semana_inicio = cursor
        semana_fin = min(cursor + timedelta(days=6), fecha_fin)

        fi = semana_inicio.strftime("%Y-%m-%dT00:00:00Z")
        ff = semana_fin.strftime("%Y-%m-%dT23:59:59Z")

        print(f"Chunk {chunk}: {fi} → {ff}")

        items, query, tamanio_pagina, request_payload, page_number = _fetch_qb_data(
            realm_id, access_token, base_url, minor_version, fi, ff
        )

        ingested_at = kwargs.get("execution_date")

        for f in items:
            todos_items.append({
                "id": f.get("Id"),
                "payload": f,
                "ingested_at_utc": ingested_at,
                "extract_window_start_utc": fi,
                "extract_window_end_utc": ff,
                "page_number": page_number,
                "page_size": tamanio_pagina,
                "request_payload": request_payload,
            })

        print(f"Chunk {chunk} devolvió {len(items)} items")
        chunk += 1
        cursor = semana_fin + timedelta(days=1)

    print(f"Total items: {len(todos_items)}")

    return {"items": todos_items}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
