from odoo.upgrade.util import spreadsheet


def migrate(cr, version):

    spreadsheet.update_spreadsheets_table_changes(cr)
    # spreadsheet.fields.rename_fields(cr)
    # spreadsheet.models.remove_models(cr)
