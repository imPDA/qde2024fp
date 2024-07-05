from typing import Any

from rdkit import Chem, DataStructs
from rdkit.Chem import AllChem
from rdkit.DataStructs import ExplicitBitVect


def calculate_morgan_fingerprints_base64(smiles: str) -> str:
    fingerprint_generator = AllChem.GetMorganGenerator(radius=2, fpSize=2048)
    try:
        molecule = Chem.MolFromSmiles(smiles)
        fingerprint = fingerprint_generator.GetFingerprint(molecule)
    except Exception as e:
        print(f'Error occurred: {e}')

        return ''

    return fingerprint.ToBase64()


def calculate_morgan_fingerprints_from_inchi_base64(inchi: str) -> str:
    fingerprint_generator = AllChem.GetMorganGenerator(radius=2, fpSize=2048)
    try:
        molecule = Chem.MolFromInchi(inchi)
        fingerprint = fingerprint_generator.GetFingerprint(molecule)
    except Exception as e:
        print(f'Error occurred: {e}')

        return ''

    return fingerprint.ToBase64()


def calculate_morgan_fingerprints(smiles: str):
    fingerprint_generator = AllChem.GetMorganGenerator(radius=2, fpSize=2048)
    molecule = Chem.MolFromSmiles(smiles)
    return fingerprint_generator.GetFingerprint(molecule)


def fingerprints_from_base64(base64_fingerprints: str):
    fingerprints = ExplicitBitVect(2048)
    fingerprints.FromBase64(base64_fingerprints)

    return fingerprints


def calculate_tanimoto_similarity(left: Any, right: Any) -> float:
    if isinstance(left, str):
        left = fingerprints_from_base64(left)
    if isinstance(right, str):
        right = fingerprints_from_base64(right)

    return DataStructs.TanimotoSimilarity(left, right)
