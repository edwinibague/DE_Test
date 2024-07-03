from prefect_gcp import GcpCredentials

service_account_info = {
  "type": "service_account",
  "project_id": "te-test-428005",
  "private_key_id": "35ac8b98f3046fb5bdd6e600f8e0ee5e4279103f",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3ns/oFhx4RAa4\n7kBBcya4DxuuCRCoI3UyKIXtKHOQ5I84wzadQBFxnD8F6g21Y5nVACh8hy4VY/aD\nu7VBIiR9ejRVux2yTPUmb9SZKkcPd4qrDvrXJigkBDi0nFqCITp6UA7dm/1QlUQG\nFo7arugDYDebQ3fgNsAVevO6vc4bTm8Y9qlOuj/sXI2teIF0Em7bv5cvIrRiI35r\nXy+wOzpeDicOtu8FwXYTshRqHLq27vMJn1BLAkCmeIiZYN2F0x9kAh45e754EK8m\nBCL2rXbrkpzpjRDTXRNXq/NGXSJ+RIvEkRrT6S9uKfrx9LQUTWkkjMMYDyEaZuPa\nWZdrAB8dAgMBAAECggEAT31FvdAMpRe13UrusCa8ZUPNFMtB1bf73SnjyGYRmXEI\ne2cu9mYs5wTf15yaMKMcjjQSUs5macYrip2w+rBgIZ3MmCx3z583JVuWKnQ97Pkw\nI5tJhegzylu1fKKPH/Roj1inBtKnl8H2f7g2QX8kW4nEqMdNVtf80Y+3gWj3l6Ph\nxUMVxs3a7VyP1ypRxpHPgY+Z+IV3XnhJQQt8UMgZuSkWcJgHte2+97o4DjQHOXV/\nB3xLo6RhhOCQ8X9bdzKmeTcigVIEjolc1e0FyMBfY/q8OOE46z01M2QP5utBRNvu\nPEFLBvwA9Ne5guaQ8tRwtH88FCZWSl/7igV+bs9A1QKBgQDkbHkHpqW2cSdQYuRZ\nPXwvICmxQm+fmNCVeLkg5sB59G5LXRD05vunfEmWVKZzPvgzVMonXEh//daweRzc\nXiVlM4nJ7nm8O4aA5+KGkMGGxYPT3x17wVD0Aiuaa6DmlkeaX6y9vkqq5cyO/VsH\nv0jFjqGYgfBbnhY9h6xeY29NRwKBgQDNyaxjzYe9atzDn6YhzHVnSZx9R8I7eEnq\njQ7FF2IF8Zt1WsSN4thHU/LEetQ51Ht1iPesQhd3k6JcNWCgcoH+jRIWu2EY7NHU\nz2hdf66GM/PXBXXD3HTzfABbpI7YWeMpm4qY6ArQ3LVlSmbyD4IVyPkViBXQ+9/8\nTlFvBbESewKBgGxvTfki1BHKn3Yaq/ntJTGd3Azo0+J4gbQjLgb/Y6nBLkaadH09\n1YMJjklOAAI/h0We36RVu5j+4Fa/98Jo8uma0LNKgQGpZE13RuLIfwZCZzVB+lYQ\nTCS5jwdsRmKQPod7GZ6tYfbExhmvDfKcjKgz8GRccsSbREWLTXk1TNXrAoGBALnq\nIVxpKuDt0lTJvwV5+fc+gaqNUeT4X48PsPBOl7hW9uOnBTxKXKrUJNdtQJ6eYItn\nGpr9esYFEwHLF2jdCqQ2Pslar/YcvdVNLDSMTdfgx8LSo/o3CVGQDaK9oG/FhXzp\nkOVjfaIEQ70m4qp9aEDWOHJDZ7JOW6VN/DQZKEhRAoGBALh4yC3dmLEYdlEpKn4I\nZ8UUfjr/MEMWwfHP82XPArqjAknBkKXKi9V/hiSbQf40Da98ARyBKMKE/qKlWOtr\n5NXRSeQBYHcHV+g10BaguE08mrbzrhvUtGAUlpdxKoOyrt3ruCgToee9/Soo04xJ\nxueirYh73KoAQ06wyWMA6Fum\n-----END PRIVATE KEY-----\n",
  "client_email": "storage-te-test@te-test-428005.iam.gserviceaccount.com",
  "client_id": "116838589843849928095",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/storage-te-test%40te-test-428005.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

gcp_credentials_block = GcpCredentials(
    service_account_info=service_account_info
).get_bigquery_client()

project_id = "te-test-428005"
