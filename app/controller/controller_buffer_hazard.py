from flask import Blueprint, jsonify, request
from app.service.service_buffer_hazard import BufferDisasterService

bp = Blueprint("buffer_disaster", __name__, url_prefix="/api/buffer")

@bp.route("/<dtype>", methods=["GET"])
def get_buffer(dtype):
    # parse bbox & tol
    
    try:
        bbox = {
            "minlng": float(request.args["minlng"]),
            "minlat": float(request.args["minlat"]),
            "maxlng": float(request.args["maxlng"]),
            "maxlat": float(request.args["maxlat"])
        }
    except (KeyError, ValueError):
        return jsonify({"error":"Parameter bbox (minlng,minlat,maxlng,maxlat) wajib numeric"}), 400

    try:
        tol = float(request.args.get("tol", 0.001))
    except ValueError:
        tol = 0.001

    field = request.args.get("field")
    if not field:
        return jsonify({"error":"Parameter field wajib"}),400
    fc = BufferDisasterService.get_feature_collection(dtype, field, bbox, tol)
    return jsonify(fc)

@bp.route("/<dtype>/nearest", methods=["GET"])
def get_nearest(dtype):
    field = request.args.get("field")
    if not field:
        return jsonify({"error":"Parameter field wajib"}), 400
    try:
        lat = float(request.args["lat"])
        lng = float(request.args["lng"])
    except (KeyError, ValueError):
        return jsonify({"error":"lat & lng wajib numeric"}), 400

    data = BufferDisasterService.get_nearest(dtype, field, lat, lng)
    if not data:
        return jsonify({"error":"tidak ditemukan"}), 404
    return jsonify(data)
