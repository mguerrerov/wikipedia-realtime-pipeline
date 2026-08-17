# Kinesis Data Streams. Equivale a Redpanda en local, y es la unica pieza que
# obliga a tener dos implementaciones de la fuente de eventos.

resource "aws_kinesis_stream" "cambios" {
  name = "${var.prefijo}-cambios"

  # PROVISIONED y no ON_DEMAND, a proposito. Bajo demanda cobra una cuota base
  # por hora bastante mas alta y solo compensa con trafico irregular o grande.
  # Aqui el caudal esta medido y es constante: 37,4 ev/s de media, pico de 114,
  # unos 53 KB/s. Un shard admite 1 MB/s y 1.000 registros por segundo, o sea
  # veinte veces lo que necesitamos.
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  shard_count = var.shards

  # 24 horas es el minimo y no se factura aparte. Ampliarlo tiene coste por
  # GB-hora, y este proyecto no reprocesa nada de mas de un dia.
  retention_period = var.retencion_horas

  # Cifrado con la clave gestionada por AWS para Kinesis: sin coste adicional,
  # a diferencia de una clave propia de KMS.
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  # Las metricas por shard se facturan como metricas personalizadas de
  # CloudWatch. Con un shard no aportan nada que no se vea en la consola.
  shard_level_metrics = []
}
