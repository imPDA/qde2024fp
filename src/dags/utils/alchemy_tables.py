from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ChemblIdLookup(Base):
    __tablename__ = 'chembl_id_lookup'

    chembl_id: str = Column(String(20), primary_key=True, nullable=False)
    entity_type: str = Column(String(50), nullable=False)
    entity_id: int = Column(Integer, nullable=False)
    status: str = Column(String(10), nullable=False)
    last_active: int = Column(Integer)


class MoleculeDictionary(Base):
    __tablename__ = 'molecule_dictionary'

    molregno: int = Column(Integer, primary_key=True, nullable=False)
    pref_name: str = Column(String(255))
    chembl_id: str = Column(String(20), nullable=False)
    max_phase: int = Column(Integer)
    therapeutic_flag: int = Column(Integer, nullable=False)
    dosed_ingredient: int = Column(Integer, nullable=False)
    structure_type: str = Column(String(10), nullable=False)
    chebi_par_id: int = Column(Integer)
    molecule_type: str = Column(String(30))
    first_approval: int = Column(Integer)
    oral: int = Column(Integer, nullable=False)
    parenteral: int = Column(Integer, nullable=False)
    topical: int = Column(Integer, nullable=False)
    black_box_warning: int = Column(Integer, nullable=False)
    natural_product: int = Column(Integer, nullable=False)
    first_in_class: int = Column(Integer, nullable=False)
    chirality: int = Column(Integer, nullable=False)
    prodrug: int = Column(Integer, nullable=False)
    inorganic_flag: int = Column(Integer, nullable=False)
    usan_year: int = Column(Integer)
    availability_type: int = Column(Integer)
    usan_stem: str = Column(String(50))
    polymer_flag: int = Column(Integer)
    usan_substem: str = Column(String(50))
    usan_stem_definition: str = Column(String(1000))
    indication_class: str = Column(String(1000))
    withdrawn_flag: int = Column(Integer, nullable=False)
    chemical_probe: int = Column(Integer, nullable=False)
    orphan: int = Column(Integer, nullable=False)


class CompoundProperties(Base):
    __tablename__ = 'compound_properties'

    molregno: int = Column(Integer, primary_key=True, nullable=False)
    mw_freebase: int = Column(Integer)
    alogp: int = Column(Integer)
    hba: int = Column(Integer)
    hbd: int = Column(Integer)
    psa: int = Column(Integer)
    rtb: int = Column(Integer)
    ro3_pass: str = Column(String(3))
    num_ro5_violations: int = Column(Integer)
    cx_most_apka: int = Column(Integer)
    cx_most_bpka: int = Column(Integer)
    cx_logp: int = Column(Integer)
    cx_logd: int = Column(Integer)
    molecular_species: str = Column(String(50))
    full_mwt: int = Column(Integer)
    aromatic_rings: int = Column(Integer)
    heavy_atoms: int = Column(Integer)
    qed_weighted: int = Column(Integer)
    mw_monoisotopic: int = Column(Integer)
    full_molformula: str = Column(String(100))
    hba_lipinski: int = Column(Integer)
    hbd_lipinski: int = Column(Integer)
    num_lipinski_ro5_violations: int = Column(Integer)
    np_likeness_score: int = Column(Integer)


class CompoundStructures(Base):
    __tablename__ = 'compound_structures'

    molregno: int = Column(Integer, primary_key=True, nullable=False)
    molfile: str = Column(Text)
    standard_inchi: str = Column(String(4000))
    standard_inchi_key: str = Column(String(27), nullable=False)
    canonical_smiles: str = Column(String(4000))
