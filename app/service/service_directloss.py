# service_directloss.py

import os
import sys
import math
import numpy as np
import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.models_database import HasilProsesDirectLoss, HasilAALProvinsi
from app.repository.repo_directloss import get_bangunan_data, get_all_disaster_data

# UTF-8 for console/logging
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Setup logger
DEBUG_DIR = os.path.join(os.getcwd(), "debug_output")
os.makedirs(DEBUG_DIR, exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.join(DEBUG_DIR, "service_directloss.log"), encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)


def process_all_disasters():
    logger.debug("=== START process_all_disasters ===")

    # Clear old
    try:
        db.session.query(HasilProsesDirectLoss).delete()
        db.session.query(HasilAALProvinsi).delete()
        db.session.commit()
        logger.debug("✅ Cleared DirectLoss & AAL")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Clearing old failed: {e}")

    # 1) Building data (with integer jumlah_lantai)
    bld = get_bangunan_data()
    logger.debug(f"📥 Buildings: {len(bld)} rows")
    if 'kode_bangunan' not in bld.columns or bld['kode_bangunan'].isna().all():
        bld['kode_bangunan'] = (
            bld['id_bangunan'].astype(str)
               .str.split('_').str[0]
               .str.lower()
        )
        logger.debug("🔧 Derived kode_bangunan from id_bangunan")
    bld['jumlah_lantai'] = bld['jumlah_lantai'].fillna(0).astype(int)
    bld['luas'] = bld['luas'].fillna(0)
    bld['hsbgn'] = bld['hsbgn'].fillna(0)

    luas  = bld['luas'].to_numpy()
    hsbgn = bld['hsbgn'].to_numpy()

    # 2) Hazard data (reindexed to bld.index!)
    disaster_data = get_all_disaster_data()
    for name, df in disaster_data.items():
        # fill na, then reindex so length==len(bld)
        df = df.fillna(0).reindex(bld.index).fillna(0)
        disaster_data[name] = df
        logger.debug(f"📥 {name}: {len(df)} rows (aligned to {len(bld)})")

    # 3) Direct loss calc
    prefix_map = {"gempa":"mmi","banjir":"depth","longsor":"mflux","gunungberapi":"kpa"}
    scales_map = {
      "gempa": ["500","250","100"],
      "banjir": ["100","50","25"],
      "longsor": ["5","2"],
      "gunungberapi": ["250","100","50"]
    }

    for name, df_raw in disaster_data.items():
        pre    = prefix_map[name]
        scales = scales_map[name]
        if name == "banjir":
            floors = np.clip(bld['jumlah_lantai'].to_numpy(), 1, 2)
            for s in scales:
                y1 = df_raw[f"nilai_y_1_{pre}{s}"].to_numpy()
                y2 = df_raw[f"nilai_y_2_{pre}{s}"].to_numpy()
                v = np.where(floors == 1, y1, y2)
                col = f"direct_loss_{name}_{s}"
                bld[col] = luas * hsbgn * v
                bld[col] = bld[col].fillna(0)
                logger.debug(f"{col} sample: {bld[col].head(3).tolist()}")
        else:
            for s in scales:
                ycols = [
                    f"nilai_y_cr_{pre}{s}",
                    f"nilai_y_mcf_{pre}{s}",
                    f"nilai_y_mur_{pre}{s}",
                    f"nilai_y_lightwood_{pre}{s}"
                ]
                maxv = df_raw[ycols].to_numpy().max(axis=1)
                col = f"direct_loss_{name}_{s}"
                bld[col] = luas * hsbgn * maxv
                bld[col] = bld[col].fillna(0)
                logger.debug(f"{col} sample: {bld[col].head(3).tolist()}")

    # 4) Save Direct Loss
    dl_cols = [c for c in bld.columns if c.startswith("direct_loss_")]

    bld = bld.drop_duplicates(subset='id_bangunan', keep='last')    

    mappings = [
        {"id_bangunan": row['id_bangunan'], **{c: row[c] for c in dl_cols}}
        for _, row in bld.iterrows()
    ]
    try:
        db.session.bulk_insert_mappings(HasilProsesDirectLoss, mappings)
        db.session.commit()
        logger.info("✅ Direct Loss saved")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Saving Direct Loss failed: {e}")
        raise

    # 5) Dump CSV & 6) AAL
    csv_path = os.path.join(DEBUG_DIR, "directloss_all.csv")
    cols_to_dump = ["provinsi", "kode_bangunan"] + dl_cols
    bld.to_csv(csv_path, index=False, sep=';', columns=cols_to_dump)
    logger.debug(f"📄 CSV DirectLoss subset for AAL: {csv_path}")

    calculate_aal()
    logger.debug("=== END process_all_disasters ===")
    return csv_path

def calculate_aal():
    path = os.path.join(DEBUG_DIR, "directloss_all.csv")
    if not os.path.exists(path):
        logger.error("❌ directloss_all.csv not found")
        return

    df = pd.read_csv(path, delimiter=';').fillna(0)

    periods = {
      "gempa_500":0.02, "gempa_250":0.04, "gempa_100":0.10,
      "banjir_100":0.05,"banjir_50":0.10,"banjir_25":0.20,
      "gunungberapi_250":0.01,"gunungberapi_100":0.03,"gunungberapi_50":0.05,
      "longsor_5":0.02,"longsor_2":0.04
    }

    dl_cols = [c for c in df.columns if c.startswith("direct_loss_")]
    grp = df.groupby(["provinsi", "kode_bangunan"]).sum()[dl_cols]
    logger.debug(f"grp (provinsi,kode_bangunan) shape: {grp.shape}")

    aal = pd.DataFrame(index=grp.index)
    for key, p in periods.items():
        dis, sc = key.split("_")
        dlc = f"direct_loss_{dis}_{sc}"
        aalc = f"aal_{dis}_{sc}"
        aal[aalc] = grp[dlc] * (-np.log(1-p))
    aal.reset_index(inplace=True)
    aal = aal.fillna(0)
    logger.debug(f"AAL before pivot: {aal.shape}")

    pivot = aal.pivot(index='provinsi', columns='kode_bangunan')
    pivot.columns = [f"{col[0]}_{col[1].lower()}" for col in pivot.columns]
    pivot.reset_index(inplace=True)
    pivot = pivot.fillna(0)
    logger.debug(f"pivot shape: {pivot.shape}")

    for key in periods.keys():
        pattern = f"aal_{key}_"
        cols = [c for c in pivot.columns if c.startswith(pattern) and not c.endswith("_total")]
        pivot[f"{pattern}total"] = pivot[cols].sum(axis=1)
    pivot = pivot.fillna(0)
    logger.debug(f"pivot with totals shape: {pivot.shape}")

    totals = pivot.select_dtypes(include=[np.number]).sum().to_dict()
    totals["provinsi"] = "Total Keseluruhan"
    final = pd.concat([pivot, pd.DataFrame([totals])], ignore_index=True).fillna(0)

    out = os.path.join(DEBUG_DIR, "AAL_per_provinsi_filtered.csv")
    final.to_csv(out, index=False, sep=';')
    logger.debug(f"📄 CSV AAL: {out}")

    try:
        db.session.query(HasilAALProvinsi).delete()
        db.session.bulk_insert_mappings(HasilAALProvinsi, final.to_dict('records'))
        db.session.commit()
        logger.info("✅ AAL saved")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Saving AAL failed: {e}")


def recalc_building_directloss_and_aal(bangunan_id: str):
    """
    Incremental recalc direct loss + AAL untuk satu bangunan tertentu.
    """
    logger.debug(f"=== START incremental recalc for {bangunan_id} ===")

    bld = get_bangunan_data()
    if 'kode_bangunan' not in bld.columns or bld['kode_bangunan'].isna().all():
        bld['kode_bangunan'] = bld['id_bangunan'].astype(str).str.split('_').str[0].str.lower()
    bld['jumlah_lantai'] = bld['jumlah_lantai'].fillna(0).astype(int)
    bld['luas']          = bld['luas'].fillna(0)
    bld['hsbgn']         = bld['hsbgn'].fillna(0)

    row = bld[bld['id_bangunan'] == bangunan_id]
    if row.empty:
        raise ValueError(f"Bangunan {bangunan_id} tidak ditemukan")
    idx = row.index[0]
    prov      = row.loc[idx, 'provinsi']
    kode_bgn  = row.loc[idx, 'kode_bangunan'].lower()
    luas_val  = row.loc[idx, 'luas']
    hsbgn_val = row.loc[idx, 'hsbgn']
    floors_val= np.clip(row.loc[idx, 'jumlah_lantai'], 1, 2)

    disaster_data = get_all_disaster_data()
    prefix_map = {"gempa":"mmi","banjir":"depth","longsor":"mflux","gunungberapi":"kpa"}
    scales_map = {
      "gempa": ["500","250","100"],
      "banjir": ["100","50","25"],
      "longsor": ["5","2"],
      "gunungberapi": ["250","100","50"]
    }

    direct_losses = {}
    for name, df_raw in disaster_data.items():
        df = df_raw.fillna(0)
        pre = prefix_map[name]
        for s in scales_map[name]:
            dlc = f"direct_loss_{name}_{s}"
            if name == "banjir":
                y1 = df.at[idx, f"nilai_y_1_{pre}{s}"]
                y2 = df.at[idx, f"nilai_y_2_{pre}{s}"]
                v  = y1 if floors_val == 1 else y2
            else:
                ycols = [
                    f"nilai_y_cr_{pre}{s}",
                    f"nilai_y_mcf_{pre}{s}",
                    f"nilai_y_mur_{pre}{s}",
                    f"nilai_y_lightwood_{pre}{s}"
                ]
                v = df.loc[idx, ycols].max()
            direct_losses[dlc] = float(luas_val * hsbgn_val * v or 0)

    old = db.session.query(HasilProsesDirectLoss).filter_by(id_bangunan=bangunan_id).one_or_none()
    old_vals = {c: getattr(old, c) for c in direct_losses} if old else {c: 0 for c in direct_losses}

    # print("old_vals", old_vals)

    if old:
        db.session.delete(old)
        db.session.commit()
    new_rec = HasilProsesDirectLoss(id_bangunan=bangunan_id, **direct_losses)
    db.session.add(new_rec)
    db.session.commit()
    logger.debug(f"✅ DirectLoss updated for {bangunan_id}")

    periods = {
      "gempa_500":0.02, "gempa_250":0.04, "gempa_100":0.10,
      "banjir_100":0.05,"banjir_50":0.10,"banjir_25":0.20,
      "gunungberapi_250":0.01,"gunungberapi_100":0.03,"gunungberapi_50":0.05,
      "longsor_5":0.02,"longsor_2":0.04
    }

    aal_row = db.session.query(HasilAALProvinsi).filter_by(provinsi=prov).one_or_none()
    if not aal_row:
        raise RuntimeError(f"AALProvinsi untuk '{prov}' tidak ditemukan")

    for key, p in periods.items():
        dis, sc = key.split("_")
        dlc = f"direct_loss_{dis}_{sc}"
        delta = direct_losses[dlc] - old_vals.get(dlc, 0)
        delta_aal = float(delta * (-math.log(1 - p)))
        col_tax = f"aal_{dis}_{sc}_{kode_bgn}"
        col_tot = f"aal_{dis}_{sc}_total"
        setattr(aal_row, col_tax, float(getattr(aal_row, col_tax, 0)) + delta_aal)
        setattr(aal_row, col_tot, float(getattr(aal_row, col_tot, 0)) + delta_aal)
        db.session.query(HasilAALProvinsi)\
        .filter_by(provinsi=prov)\
        .update({
            col_tax: HasilAALProvinsi.__table__.c[col_tax] + delta_aal,
            col_tot: HasilAALProvinsi.__table__.c[col_tot] + delta_aal
        })
    db.session.commit()
    logger.info(f"✅ AAL incremental updated for provinsi {prov}")
    logger.debug(f"=== END incremental recalc for {bangunan_id} ===")

    return {"direct_losses": direct_losses}
