# IBKR: checklist de configuración (para que el script conecte)

Esto es lo típico para **TWS** o **IB Gateway**:

## 1) Paper Trading

- Entra en **Paper Trading** (no mezcles entornos al principio).
- Puerto típico:
  - TWS Paper suele ir en **7497**
  - TWS Live suele ir en **7496**

> Si usas otro, ponlo en `.env`.

## 2) Activar API (Socket)

En TWS/IB Gateway:
- `Settings` / `Global Configuration` / `API` / `Settings`
- Marca **Enable ActiveX and Socket Clients**
- Pon el **Socket port** (ej. 7497)
- (Opcional) **Read-Only API** si solo quieres escanear y leer datos
- (Opcional) `Trusted IPs`: añade `127.0.0.1` (y/o la IP de WSL si tienes problemas)

## 3) Mercado / permisos

- Necesitas suscripción a market data (aunque sea barata, L1 US) si quieres snapshot fiable.
- Si no la tienes, el script seguirá funcionando, pero faltarán campos y la watchlist será menos fina.

## 4) Conexión desde WSL

Casi siempre (WSL2 moderno) `IB_HOST=127.0.0.1` funciona.

Si no, alternativa:

```bash
# IP del host Windows vista desde WSL (suele ser la del nameserver)
cat /etc/resolv.conf | grep nameserver
```

Y en `.env`:

```env
IB_HOST=<esa_ip>
```
