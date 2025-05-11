import logging
import pandas as pd
from scipy.interpolate import CubicSpline
from app.repository.repo_kurva_gempa import get_reference_curves  # Ensure correct import for earthquake references
from app.extensions import db
from app.models.models_database import HasilProsesGempa  # ORM model for the 'dmgratio_gempa' table

# Setup logging
logger = logging.getLogger(__name__)

def interpolate_spline(x, y, xi):
    """Interpolasi CubicSpline dengan hasil dibatasi [0, 1]."""
    if pd.isna(xi):
        return None
    try:
        spline = CubicSpline(x, y, extrapolate=True)
        val = spline(float(xi))
        return float(max(0, min(val, 1)))
    except Exception as e:
        logger.error(f"❌ ERROR interpolasi nilai {xi}: {e}")
        return None

def process_data(input_data):
    """
    Proses data kedalaman untuk interpolasi CR, MCF, dan MUR untuk gempa.
    Kolom input: lon, lat, MMI500, MMI250, MMI100.
    Output: DataFrame hasil interpolasi.
    """
    logger.info("📥 Memulai proses interpolasi data Gempa...")

    reference_curves = get_reference_curves()
    if not reference_curves:
        logger.warning("⚠️ Tidak ada referensi kurva Gempa! Proses dihentikan.")
        return pd.DataFrame()

    df = input_data.copy()
    for col in ['MMI500', 'MMI250', 'MMI100']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Interpolasi untuk setiap tipe referensi
    for tipe, ref in reference_curves.items():
        x_ref, y_ref = ref['x'], ref['y']
        logger.info(f"📊 Referensi {tipe}: X={x_ref}, Y={y_ref}")
        for mmi in ['500', '250', '100']:
            col_in = f'MMI{mmi}'
            col_out = f'dmgratio_{tipe.lower()}_mmi{mmi}'
            df[col_out] = df[col_in].apply(lambda v: interpolate_spline(x_ref, y_ref, v))

    # Define columns for output
    cols = [
        'id_lokasi',
        'dmgratio_cr_mmi500', 'dmgratio_mcf_mmi500', 'dmgratio_mur_mmi500', 'dmgratio_lightwood_mmi500',
        'dmgratio_cr_mmi250', 'dmgratio_mcf_mmi250', 'dmgratio_mur_mmi250', 'dmgratio_lightwood_mmi250',
        'dmgratio_cr_mmi100', 'dmgratio_mcf_mmi100', 'dmgratio_mur_mmi100', 'dmgratio_lightwood_mmi100'
    ]

    result = df[cols].applymap(lambda x: float(x) if pd.notna(x) else None)
    logger.info(f"✅ Interpolasi selesai: {result.shape[0]} baris.")

    # ————————————————
    # Simpan ke database via ORM, menggunakan bulk insert untuk meningkatkan performa
    try:
        existing_ids = set([record.id_lokasi for record in db.session.query(HasilProsesGempa.id_lokasi).all()])

        # List to hold the records to be inserted or updated
        to_insert = []
        to_update = []

        for _, row in result.iterrows():
            rec = HasilProsesGempa(
                id_lokasi=int(row['id_lokasi']),
                dmgratio_cr_mmi500=float(row['dmgratio_cr_mmi500']),
                dmgratio_mcf_mmi500=float(row['dmgratio_mcf_mmi500']),
                dmgratio_mur_mmi500=float(row['dmgratio_mur_mmi500']),
                dmgratio_lightwood_mmi500=float(row['dmgratio_lightwood_mmi500']),
                dmgratio_cr_mmi250=float(row['dmgratio_cr_mmi250']),
                dmgratio_mcf_mmi250=float(row['dmgratio_mcf_mmi250']),
                dmgratio_mur_mmi250=float(row['dmgratio_mur_mmi250']),
                dmgratio_lightwood_mmi250=float(row['dmgratio_lightwood_mmi250']),
                dmgratio_cr_mmi100=float(row['dmgratio_cr_mmi100']),
                dmgratio_mcf_mmi100=float(row['dmgratio_mcf_mmi100']),
                dmgratio_mur_mmi100=float(row['dmgratio_mur_mmi100']),
                dmgratio_lightwood_mmi100=float(row['dmgratio_lightwood_mmi100']),
            )

            if rec.id_lokasi in existing_ids:
                to_update.append(rec)  # Add to update list if the id_lokasi exists
            else:
                to_insert.append(rec)  # Add to insert list if id_lokasi does not exist

        # Perform bulk insert and update
        if to_insert:
            db.session.bulk_save_objects(to_insert)  # Bulk insert
            logger.info(f"✅ {len(to_insert)} records inserted into the database.")

        if to_update:
            for rec in to_update:
                existing_record = db.session.query(HasilProsesGempa).filter_by(id_lokasi=rec.id_lokasi).first()
                existing_record.dmgratio_cr_mmi500 = rec.dmgratio_cr_mmi500
                existing_record.dmgratio_mcf_mmi500 = rec.dmgratio_mcf_mmi500
                existing_record.dmgratio_mur_mmi500 = rec.dmgratio_mur_mmi500
                existing_record.dmgratio_lightwood_mmi500 = rec.dmgratio_lightwood_mmi500
                existing_record.dmgratio_cr_mmi250 = rec.dmgratio_cr_mmi250
                existing_record.dmgratio_mcf_mmi250 = rec.dmgratio_mcf_mmi250
                existing_record.dmgratio_mur_mmi250 = rec.dmgratio_mur_mmi250
                existing_record.dmgratio_lightwood_mmi250 = rec.dmgratio_lightwood_mmi250
                existing_record.dmgratio_cr_mmi100 = rec.dmgratio_cr_mmi100
                existing_record.dmgratio_mcf_mmi100 = rec.dmgratio_mcf_mmi100
                existing_record.dmgratio_mur_mmi100 = rec.dmgratio_mur_mmi100
                existing_record.dmgratio_lightwood_mmi100 = rec.dmgratio_lightwood_mmi100

            db.session.commit()  # Commit the updates
            logger.info(f"✅ {len(to_update)} records updated in the database.")

        db.session.commit()  # Commit all inserts and updates
        logger.info("✅ Data berhasil disimpan ke tabel dmgratio_gempa.")
    except Exception as e:
        db.session.rollback()  # Rollback the session on error
        logger.error(f"❌ Gagal simpan: {e}")

    return result
