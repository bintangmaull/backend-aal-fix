import logging
import pandas as pd
from scipy.interpolate import CubicSpline
from app.repository.repo_kurva_longsor import get_reference_curves_longsor  # Ensure correct import for volcanic references
from app.extensions import db
from app.models.models_database import HasilProsesLongsor  # ORM model for the 'dmgratio_gunungberapi' table

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
    Proses data mflux_5 dan mflux_2 untuk interpolasi CR, MCF, dan MUR untuk longsor.
    Kolom input: lon, lat, mflux_5, mflux_2.
    Output: DataFrame hasil interpolasi.
    """
    logger.info("📥 Memulai proses interpolasi data longsor...")

    reference_curves = get_reference_curves_longsor()
    if not reference_curves:
        logger.warning("⚠️ Tidak ada referensi kurva longsor! Proses dihentikan.")
        return pd.DataFrame()

    df = input_data.copy()
    for col in ['mflux_5', 'mflux_2']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Interpolasi untuk setiap tipe referensi
    for tipe, ref in reference_curves.items():
        x_ref, y_ref = ref['x'], ref['y']
        logger.info(f"📊 Referensi {tipe}: X={x_ref}, Y={y_ref}")
        for mflux in ['5', '2']:
            col_in = f'mflux_{mflux}'
            col_out = f'dmgratio_{tipe.lower()}_mflux{mflux}'
            df[col_out] = df[col_in].apply(lambda v: interpolate_spline(x_ref, y_ref, v))

    # Define columns for output
    cols = [
        'id_lokasi',
        'dmgratio_cr_mflux5', 'dmgratio_mcf_mflux5', 'dmgratio_mur_mflux5', 'dmgratio_lightwood_mflux5',
        'dmgratio_cr_mflux2', 'dmgratio_mcf_mflux2', 'dmgratio_mur_mflux2', 'dmgratio_lightwood_mflux2'
    ]

    result = df[cols].applymap(lambda x: float(x) if pd.notna(x) else None)
    logger.info(f"✅ Interpolasi selesai: {result.shape[0]} baris.")

    # Save results to the database using ORM, using bulk insert for better performance
    try:
        existing_ids = set([record.id_lokasi for record in db.session.query(HasilProsesLongsor.id_lokasi).all()])

        # List to hold the records to be inserted or updated
        to_insert = []
        to_update = []

        for _, row in result.iterrows():
            rec = HasilProsesLongsor(
                id_lokasi=int(row['id_lokasi']),
                dmgratio_cr_mflux5=float(row['dmgratio_cr_mflux5']),
                dmgratio_mcf_mflux5=float(row['dmgratio_mcf_mflux5']),
                dmgratio_mur_mflux5=float(row['dmgratio_mur_mflux5']),
                dmgratio_lightwood_mflux5=float(row['dmgratio_lightwood_mflux5']),
                dmgratio_cr_mflux2=float(row['dmgratio_cr_mflux2']),
                dmgratio_mcf_mflux2=float(row['dmgratio_mcf_mflux2']),
                dmgratio_mur_mflux2=float(row['dmgratio_mur_mflux2']),
                dmgratio_lightwood_mflux2=float(row['dmgratio_lightwood_mflux2']),
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
                existing_record = db.session.query(HasilProsesLongsor).filter_by(id_lokasi=rec.id_lokasi).first()
                existing_record.dmgratio_cr_mflux5 = rec.dmgratio_cr_mflux5
                existing_record.dmgratio_mcf_mflux5 = rec.dmgratio_mcf_mflux5
                existing_record.dmgratio_mur_mflux5 = rec.dmgratio_mur_mflux5
                existing_record.dmgratio_lightwood_mflux5 = rec.dmgratio_lightwood_mflux5
                existing_record.dmgratio_cr_mflux2 = rec.dmgratio_cr_mflux2
                existing_record.dmgratio_mcf_mflux2 = rec.dmgratio_mcf_mflux2
                existing_record.dmgratio_mur_mflux2 = rec.dmgratio_mur_mflux2
                existing_record.dmgratio_lightwood_mflux2 = rec.dmgratio_lightwood_mflux2

            db.session.commit()  # Commit the updates
            logger.info(f"✅ {len(to_update)} records updated in the database.")

        db.session.commit()  # Commit all inserts and updates
        logger.info("✅ Data berhasil disimpan ke tabel longsor.")
    except Exception as e:
        db.session.rollback()  # Rollback the session on error
        logger.error(f"❌ Gagal simpan: {e}")

    return result
