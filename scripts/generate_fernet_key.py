"""Génère une clé Fernet à coller dans .env (AIRFLOW__CORE__FERNET_KEY=...).

Cette clé chiffre les connexions/variables sensibles stockées par Airflow
dans sa base de métadonnées. Usage :

    python3 scripts/generate_fernet_key.py
"""
import base64
import os

if __name__ == "__main__":
    # Une clé Fernet est simplement 32 octets aléatoires encodés en base64 urlsafe.
    # Génération sans dépendance externe (pas besoin d'installer 'cryptography').
    key = base64.urlsafe_b64encode(os.urandom(32))
    print(key.decode())
