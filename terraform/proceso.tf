# EMR Serverless. Equivale a los contenedores de Spark en local.
#
# SIN CONFIGURACION DE RED, Y ES DELIBERADO.
#
# Una aplicacion de EMR Serverless solo necesita ir dentro de una VPC si tiene
# que alcanzar recursos que viven en una VPC: una base de datos RDS, un
# ElastiCache, un endpoint privado. Aqui todo lo que consume el job -S3, Glue y
# Kinesis- son servicios de AWS accesibles por la red gestionada del propio
# servicio.
#
# Meterlo en una VPC obligaria a una de estas dos cosas:
#   - un NAT Gateway, unos 32 EUR al mes mas trafico, o
#   - endpoints de VPC para S3, Glue y Kinesis, que son tres recursos mas y,
#     salvo el de S3, se facturan por hora.
#
# Las dos sobran para lo que hace este proyecto. Al no declarar
# `network_configuration`, no se crea ninguna de las dos y la factura de red es
# cero. Si algun dia el pipeline tuviera que leer de una base de datos privada,
# habria que anadir VPC y entonces si tocaria decidir entre NAT y endpoints.

resource "aws_emrserverless_application" "spark" {
  name = "${var.prefijo}-spark"
  type = "spark"

  # Misma version que fija el entorno local. Ver docs/versiones.md: trae Spark
  # 3.5.6, Iceberg 1.10.0 y Corretto 17, y el conector de Kinesis para
  # Structured Streaming ya viene en la imagen desde EMR 7.1.
  release_label = "emr-7.13.0"

  architecture = "X86_64"

  # Que arranque sola al enviar un job evita tener que acordarse de encenderla.
  auto_start_configuration {
    enabled = true
  }

  # El freno de coste mas importante del proyecto.
  #
  # Cuidado con lo que NO hace: un job de streaming en marcha nunca esta
  # ocioso, asi que esto no lo va a parar. Lo que evita es pagar por una
  # aplicacion encendida despues de que los jobs terminen o fallen.
  # Parar el job de streaming es un paso manual y esta documentado en el README.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.minutos_parada_automatica
  }

  # Techo duro de recursos. EMR Serverless factura por vCPU-hora y GB-hora de
  # lo que realmente use, pero sin tope un job mal dimensionado puede escalar
  # mucho mas de lo previsto. Con estos valores, el peor caso es acotado.
  maximum_capacity {
    cpu    = "${var.capacidad_maxima_vcpu} vCPU"
    memory = "${var.capacidad_maxima_memoria_gb} GB"
  }

  # Sin `initial_capacity`: los trabajadores preinicializados se cobran desde
  # que la aplicacion arranca, esten haciendo algo o no. Arrancan mas rapido, y
  # ese arranque no nos importa nada aqui.
}
