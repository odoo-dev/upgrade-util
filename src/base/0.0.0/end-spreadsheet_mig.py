from odoo.upgrade.util import spreadsheet


def migrate(cr, version):

    spreadsheet.fields.rename_fields(cr)
    spreadsheet.models.remove_models(cr)
