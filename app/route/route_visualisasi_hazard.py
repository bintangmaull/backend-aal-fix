import os
from flask import Blueprint
from app.controller.controller_visualisasi_hazard import (
    get_reference_curves,
    get_density_raster,
    reload_all_rasters
)

visualisasi_bp = Blueprint(
    'visualisasi_hazard',
    __name__,
    url_prefix='/api/visualisasi'
)

# Register each endpoint here
visualisasi_bp.add_url_rule(
    '/<hazard_type>/curves',
    endpoint='get_reference_curves',
    view_func=get_reference_curves,
    methods=['GET']
)

visualisasi_bp.add_url_rule(
    '/<hazard_type>/raster',
    endpoint='get_density_raster',
    view_func=get_density_raster,
    methods=['GET']
)

visualisasi_bp.add_url_rule(
    '/reload',
    endpoint='reload_all_rasters',
    view_func=reload_all_rasters,
    methods=['POST']
)

def register_visualisasi_routes_hazard(app):
    app.register_blueprint(visualisasi_bp)
