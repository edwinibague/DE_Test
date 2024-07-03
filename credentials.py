from prefect_gcp import GcpCredentials
gcp_credentials_block = GcpCredentials.load("detest", validate=False)

project_id = "te-test-428005"
