# app/repository/repo_crud_bangunan.py

from sqlalchemy import insert
from app.models.models_database import Bangunan
from app.extensions import db
from app.repository.repo_directloss import get_ids_by_kota

class BangunanRepository:
    # Daftar kolom non-geom untuk SELECT — ditambahkan jumlah_lantai
    _fields = [
        "id_bangunan",
        "lon",
        "lat",
        "taxonomy",
        "luas",
        "jumlah_lantai",    # ← baru
        "nama_gedung",
        "alamat",
        "kota",
        "provinsi"
    ]
    _columns = [getattr(Bangunan, f) for f in _fields]

    @staticmethod
    def exists_id(bangunan_id: str) -> bool:
        """Cek apakah id_bangunan sudah ada di DB."""
        return db.session.query(
            db.exists().where(Bangunan.id_bangunan == bangunan_id)
        ).scalar()

    @staticmethod
    def get_all(provinsi=None, kota=None, nama=None):
        """
        Ambil list bangunan (tanpa geom) dengan optional filter.
        """
        q = db.session.query(*BangunanRepository._columns)
        if provinsi:
            q = q.filter(Bangunan.provinsi == provinsi)
        if kota:
            q = q.filter(Bangunan.kota == kota)
        if nama:
            q = q.filter(Bangunan.nama_gedung.ilike(f"%{nama}%"))
        rows = q.order_by(Bangunan.nama_gedung).all()
        return [dict(zip(BangunanRepository._fields, row)) for row in rows]

    @staticmethod
    def get_by_id(bangunan_id):
        """
        Ambil satu bangunan berdasarkan ID (tanpa geom).
        """
        q = db.session.query(*BangunanRepository._columns)
        row = q.filter(Bangunan.id_bangunan == bangunan_id).first()
        return dict(zip(BangunanRepository._fields, row)) if row else None

    @staticmethod
    def get_provinsi_list():
        """
        Ambil daftar provinsi unik.
        """
        rows = (
            db.session.query(Bangunan.provinsi)
            .distinct()
            .order_by(Bangunan.provinsi)
            .all()
        )
        return [r[0] for r in rows]

    @staticmethod
    def get_kota_list(provinsi):
        """
        Ambil daftar kota unik berdasarkan provinsi.
        """
        rows = (
            db.session.query(Bangunan.kota)
            .filter(Bangunan.provinsi == provinsi)
            .distinct()
            .order_by(Bangunan.kota)
            .all()
        )
        return [r[0] for r in rows]

    @staticmethod
    def create(data):
        """
        INSERT record baru hanya untuk kolom non-geom.
        Postgres akan generate geom otomatis.
        """
        insert_data = {f: data[f] for f in BangunanRepository._fields if f in data}
        stmt = insert(Bangunan).values(**insert_data)
        db.session.execute(stmt)
        db.session.commit()
        return BangunanRepository.get_by_id(insert_data["id_bangunan"])

    @staticmethod
    def update(bangunan_id, data):
        """
        UPDATE via ORM. geom akan di-recompute di DB.
        """
        b = Bangunan.query.get(bangunan_id)
        if not b:
            return None
        data.pop("id_bangunan", None)
        data.pop("geom", None)
        for k, v in data.items():
            setattr(b, k, v)
        db.session.commit()
        return BangunanRepository.get_by_id(bangunan_id)

    @staticmethod
    def delete(bangunan_id):
        """
        DELETE record.
        """
        b = Bangunan.query.get(bangunan_id)
        if not b:
            return False
        db.session.delete(b)
        db.session.commit()
        return True

    @staticmethod
    def recalc_by_kota(kota: str) -> dict[str, dict]:
        """
        Untuk setiap bangunan di `kota`, panggil recalc_building_directloss_and_aal.
        Kembalikan dict { id_bangunan: hasil_recalc }.
        """
        # lazy-import untuk menghindari circular dependency
        from app.service.service_crud_bangunan import BangunanService

        ids = get_ids_by_kota(kota)
        results = {}
        for bid in ids:
            # panggil service yang sudah ada
            res = BangunanService.recalc_building_directloss_and_aal(bid)
            results[bid] = res
        return results
