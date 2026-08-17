# Coste en AWS

**Todo lo de este documento son estimaciones a partir de precios publicados el
18/08/2026, no mediciones.** La cifra real saldrá del panel de facturación tras
la sesión de validación y se registrará en `docs/metrics.md`, que es el único
sitio del que pueden salir números para el README.

Presupuesto del proyecto entero: **menos de 15 €**.

## Qué se factura y qué no

| Recurso | Se cobra | Precio publicado |
|---|---|---|
| EMR Serverless | Solo mientras ejecuta un job | 0,052624 $/vCPU-hora y 0,0057785 $/GB-hora |
| Kinesis, un shard | **Siempre, exista tráfico o no** | 0,015 $/shard-hora |
| Kinesis, registros | Por unidad de 25 KB | 0,014 $ por millón |
| S3, almacenamiento | Por GB-mes | Céntimos con estos volúmenes |
| S3, peticiones | Por millar de PUT | Relevante: el streaming escribe muchos ficheros pequeños |
| Glue Data Catalog | Gratis hasta el primer millón de objetos y peticiones | 0 € aquí |
| Red | Nada: no hay VPC, ni NAT, ni endpoints | 0 € |

Precios de referencia de la región de EE. UU.; en Estocolmo pueden variar
ligeramente. EMR redondea al segundo con un mínimo de un minuto por job.

## Sesión de validación de dos horas

Configuración prevista: 6 vCPU y 12 GB entre driver y ejecutores.

| Concepto | Cálculo | Estimado |
|---|---|---|
| EMR Serverless | (6 × 0,052624 + 12 × 0,0057785) × 2 h | ~0,77 $ |
| Kinesis, shard | 0,015 × 2 h | ~0,03 $ |
| Kinesis, registros | ~270.000 eventos | <0,01 $ |
| S3 | almacenamiento y peticiones | ~0,20 $ |
| **Total** | | **~1 $** |

Sobra presupuesto de largo. **El coste de la sesión no es el riesgo.**

## El riesgo real: olvidarse algo encendido

Hay una asimetría que conviene tener presente:

- **EMR Serverless ocioso no cuesta nada.** Con la parada automática, una
  aplicación encendida sin jobs no factura.
- **S3 con estos volúmenes es despreciable**, y los puntos de control expiran
  solos a los 7 días.
- **El shard de Kinesis se cobra siempre**: 0,36 $ al día, unos 11 $ al mes.
  Un olvido de seis semanas se come el presupuesto entero del proyecto.

Por eso el `terraform destroy` no es opcional y la lista de verificación
posterior es un entregable de la fase 6, no una nota al pie.

## Red de seguridad

`terraform/presupuesto.tf` crea un presupuesto de AWS con tres avisos por
correo: al 50 % del gasto real, al 90 %, y —el más útil— cuando la
**proyección** del mes supera el tope. Ese último es el que detecta un recurso
olvidado a los pocos días en vez de a final de mes.

Los dos primeros presupuestos de una cuenta no se facturan.

Un presupuesto **avisa, no impide**. No sustituye a destruir.

## Sobre el plan gratuito

Descartado: la cuenta ya había consumido esa oferta anteriormente. Se opera en
plan de pago, así que no hay tope duro y los avisos de presupuesto pasan de ser
una comodidad a ser la única red.

Conviene saber, por si aparece en otro contexto, que el plan gratuito actual no
son servicios gratis sino 200 $ en créditos durante seis meses, que ni EMR
Serverless ni Kinesis están entre los servicios siempre gratuitos, y que al
terminar los seis meses AWS cierra la cuenta.
