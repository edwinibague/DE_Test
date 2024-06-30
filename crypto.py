from cryptography.fernet import Fernet

def encrypt_file(file_path, passphrase):
    with open(file_path, "rb") as f_in:
        data = f_in.read()

    fernet = Fernet(passphrase.encode("utf-8"))
    encrypted_data = fernet.encrypt(data)

    with open(file_path + ".enc", "wb") as f_out:
        f_out.write(encrypted_data)

# Reemplace "my-secret-passphrase" con su frase de cifrado segura
passphrase = "Data_For_Me"
encrypt_file("client_secret_1.apps.googleusercontent.com.json", passphrase)
