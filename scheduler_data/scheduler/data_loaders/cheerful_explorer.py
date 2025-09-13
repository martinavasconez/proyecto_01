import time
import requests
import logging
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)


def _refresh_access_token():
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_client_secret')
    refresh_token = get_secret_value('qb_refresh_token')

    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    resp = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    resp.raise_for_status()
    tokens = resp.json()
    logger.info("Nuevo access_token obtenido vía refresh_token")
    return tokens["access_token"]


def _fetch_qb_data(realm_id, access_token, base_url, minor_version,
                   fecha_inicio, fecha_fin, tamanio_pagina=100, max_retries=5):
    """
    Devuelve la respuesta cruda de QuickBooks pero acumulando todas las páginas.
    """
    if not realm_id or not access_token:
        raise ValueError("Se requieren realm_id y access_token")
    if not base_url or not minor_version:
        raise ValueError("Se requiere base_url y minor_version")
    if not fecha_inicio or not fecha_fin:
        raise ValueError("Se requieren fecha_inicio y fecha_fin")

    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"
    pagina = 1  
    todas_facturas = []

    while True:
        query = (
            "SELECT * FROM Invoice "
            f"WHERE Metadata.LastUpdatedTime >= '{fecha_inicio}' "
            f"AND Metadata.LastUpdatedTime <= '{fecha_fin}' "
            f"STARTPOSITION {pagina} "
            f"MAXRESULTS {tamanio_pagina}"
        )

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }
        params = {"minorversion": minor_version}

        intentos = 0
        while intentos < max_retries:
            try:
                logger.info(f"Request a la API: página={pagina}, intento {intentos+1}")
                response = requests.get(url, headers=headers, params={**params, "query": query}, timeout=60)

                if response.status_code == 401:  
                    logger.warning("Access token expirado, refrescando...")
                    access_token = _refresh_access_token()
                    headers["Authorization"] = f"Bearer {access_token}"
                    intentos += 1
                    continue

                response.raise_for_status()
                data = response.json()

                lote = data.get("QueryResponse", {}).get("Invoice", [])
                todas_facturas.extend(lote)

                # si hay menos resultados que el tamaño de página → ya no hay más páginas
                if len(lote) < tamanio_pagina:
                    return {
                        "QueryResponse": {"Invoice": todas_facturas},
                        "time": data.get("time"),
                    }
                else:
                    pagina += tamanio_pagina
                    break  # sigue a la siguiente página

            except requests.exceptions.RequestException as e:
                intentos += 1
                espera = 2 ** intentos
                logger.warning(f"Error en request ({e}), reintento {intentos}/{max_retries} en {espera}s...")
                time.sleep(espera)

        else:
            raise Exception(f"Falló la request después de {max_retries} intentos")


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Loader para traer facturas de QuickBooks (respuesta cruda, todas las páginas).
    """
    realm_id = get_secret_value('qb_realm_id')
    access_token = get_secret_value('qb_access_token')
    minor_version = 75
    base_url = "https://sandbox-quickbooks.api.intuit.com"

    fecha_inicio = kwargs.get("fecha_inicio") or "2025-07-01T00:00:00-07:00"
    fecha_fin = kwargs.get("fecha_fin") or "2025-09-09T23:59:59-07:00"

    data = _fetch_qb_data(
        realm_id, access_token, base_url, minor_version, fecha_inicio, fecha_fin
    )
    logger.info(f"Loader devolvió {len(data.get('QueryResponse', {}).get('Invoice', []))} facturas")
    return data


@test
def test_output(output, *args) -> None:
    """
    Test básico para validar que el loader devuelve algo.
    """
    assert output is not None, 'El output está vacío'
    assert isinstance(output, dict), 'El output debe ser un dict (respuesta JSON cruda)'
    assert "QueryResponse" in output, 'Falta la clave QueryResponse en la respuesta'

