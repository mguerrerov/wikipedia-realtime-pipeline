# Aviso de gasto. No impide nada por si solo, pero convierte un olvido en un
# correo en vez de en una sorpresa a fin de mes.
#
# Existe porque el riesgo de este proyecto no es la sesion de validacion -que
# cuesta poco mas de un dolar- sino dejarse algo encendido. El shard de Kinesis
# se factura este o no fluyendo nada: 0,36 USD al dia, unos 11 al mes.
#
# Los dos primeros presupuestos de una cuenta no se facturan.

resource "aws_budgets_budget" "tope" {
  name         = "${var.prefijo}-tope-mensual"
  budget_type  = "COST"
  limit_amount = var.tope_gasto_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Aviso al superar la mitad de lo previsto: aun hay margen para reaccionar.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.correo_avisos]
  }

  # Aviso al 90 % de lo real.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 90
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.correo_avisos]
  }

  # El mas util de los tres: avisa cuando la *proyeccion* del mes supera el
  # tope, no cuando ya se ha gastado. Es el que detecta un recurso olvidado a
  # los pocos dias, en vez de a final de mes.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.correo_avisos]
  }
}
