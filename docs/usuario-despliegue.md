# Usuario IAM de despliegue

Terraform se ejecuta con un usuario IAM propio, nunca con las claves del
usuario root. Una clave de root no admite políticas: puede hacer cualquier cosa
en la cuenta, incluida cerrarla, y ninguna barrera del proyecto la limita.

## Pasos en la consola de AWS

Primero la política y después el usuario: el asistente de creación de usuario
solo deja adjuntar políticas que ya existan, no escribirlas.

1. **IAM → Políticas → Crear política**, pestaña **JSON**. Pega el contenido de
   `politica-despliegue.json` (en esta misma carpeta) y nómbrala
   `wikipipe-despliegue`.

2. **IAM → Usuarios → Crear usuario**. Nombre: `terraform-wikipipe`.
   **No** marques el acceso al portal de administración: este usuario solo usa
   claves. En permisos, *Adjuntar políticas directamente* → `wikipipe-despliegue`.

3. **Usuario → Credenciales de seguridad → Crear clave de acceso**.
   Caso de uso: *Interfaz de línea de comandos (CLI)*; hay que marcar la
   casilla de confirmación del aviso.
   **La clave secreta solo se muestra una vez**: descarga el `.csv` antes de
   cerrar. Si se pierde, se borra esa clave y se crea otra.

4. En una terminal normal, **no** dentro del asistente:
   ```
   aws configure --profile wikipipe
   ```
   Región `eu-north-1`, formato `json`.

5. **Borra las claves de root**: arriba a la derecha, tu nombre de cuenta →
   *Credenciales de seguridad* → sección *Claves de acceso* → eliminar.
   Mientras existan, siguen siendo el eslabón débil aunque no las uses.

6. **Activa MFA en el usuario root**, en esa misma pantalla. No afecta a este
   proyecto, pero es la protección que de verdad importa en esa cuenta.

## Qué permisos lleva y por qué

| Bloque | Para qué |
|---|---|
| S3 | Crear el bucket del almacén, su cifrado, su ciclo de vida y borrarlo |
| Kinesis | Crear, describir y borrar el stream |
| Glue | Crear y borrar las tres bases de datos del catálogo |
| EMR Serverless | Crear, describir y borrar la aplicación, y lanzar jobs |
| IAM | Crear el rol de ejecución **solo** con el prefijo del proyecto, y pasárselo a EMR |
| Budgets | Crear el presupuesto de avisos |
| STS y lectura de etiquetas | Comprobaciones que hace el propio Terraform |

El bloque de IAM está acotado por nombre de recurso: este usuario solo puede
crear y borrar roles y políticas que empiecen por `wikipipe-`. No puede tocar
nada más de IAM, ni crear usuarios, ni ampliarse sus propios permisos.

`iam:PassRole` está limitado a que el servicio destino sea EMR Serverless. Sin
esa condición, un permiso de pasar roles permite escalar privilegios entregando
un rol potente a cualquier servicio.

## Si el plan o el apply fallan por permisos

Es lo esperable con una política acotada: aparecerá un error del tipo
`AccessDenied` nombrando la acción que falta. Se añade esa acción concreta al
bloque correspondiente y se reintenta. Es preferible eso a empezar con
`AdministratorAccess`, que funciona a la primera y no se puede defender.
