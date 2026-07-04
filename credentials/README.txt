Coloque aqui o arquivo JSON da service account do GCP.
Exemplo: service-account.json

Este diretório está no .gitignore e nunca será commitado.

Permissões necessárias na service account:
- BigQuery Data Editor
- BigQuery Job User
- Pub/Sub Editor

Após salvar o arquivo, defina a variável de ambiente:
  export GOOGLE_APPLICATION_CREDENTIALS="credentials/service-account.json"

No GitHub Actions, a autenticação é feita via secret GCP_SA_KEY
configurado no step "Autenticar no GCP" do workflow.
