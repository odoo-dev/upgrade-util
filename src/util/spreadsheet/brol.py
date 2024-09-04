
import json
import time
import logging

from .fields import modify_all_fields
from .misc import _magic_spreadsheet_field, write_attachment
from .models import modify_all_models
from .revisions import transform_revisions_data
from .data_wrappers import Spreadsheet
from odoo.addons.base.maintenance.migrations import util

_logger = logging.getLogger(__name__)

def get_revisions(cr, res_model, res_id):
    cr.execute(
        """
        SELECT id, commands
          FROM spreadsheet_revision
         WHERE res_model=%s
           AND res_id=%s
        """,
        [res_model, res_id]
    )
    return cr.fetchall()

def update_spreadsheets_table_changes(cr):
    if util.table_exists(cr, "documents_document"):
        query = r"""
            SELECT doc.id AS document_id, a.id AS attachment_id, a.db_datas
              FROM documents_document doc
         LEFT JOIN ir_attachment a ON a.id = doc.attachment_id
             WHERE doc.handler='spreadsheet'
            """

        # TODO there are excel files in there!
        # for unused_doc_id, attachment_id, db_datas in cr.fetchall():
        start_total_time = time.time()
        with util.named_cursor(cr, 20) as ncr:
            ncr.execute(query)

            for doc_id, attachment_id, db_datas in ncr:
                if db_datas:
                    start_time = time.time()
                    data = json.loads(db_datas.tobytes())
                    data, model_adapters = modify_all_models(cr, data)
                    data, field_adapters = modify_all_fields(cr, data)
                    data = Spreadsheet(data).data
                    _logger.info("--- %s seconds to update ---" % (time.time() - start_time))
                    write_attachment(cr, attachment_id, data)
                    _logger.info("--- %s seconds to update  & write ---" % (time.time() - start_time))

                    all_adapters = model_adapters + field_adapters

                    start_time = time.time()
                    revisions_data = []
                    revisions_ids = []
                    for revision_id, commands in get_revisions(cr, "documents_document", doc_id):
                        ## THis is currently false, we need al reveisions from the same res_id,res_model grouped together.
                        revisions_data.append(json.loads(commands))
                        revisions_ids.append(revision_id)

                    revisions = transform_revisions_data(revisions_data, *all_adapters)
                    for revision_id, revision in zip(revisions_ids, revisions):
                        cr.execute(
                            """
                            UPDATE spreadsheet_revision
                                SET commands=%s
                                WHERE id=%s
                            """,
                            [json.dumps(revision), revision_id],
                        )
                    _logger.info("--- %s seconds to update  revisions ---" % (time.time() - start_time))


        _logger.info("--- %s seconds to update spreadsheets ---" % (time.time() - start_total_time))



    start_time = time.time()
    if util.table_exists(cr, "spreadsheet_dashboard"):
        data_field = _magic_spreadsheet_field(cr) #"spreadsheet_binary_data" if version_gte("saas~16.3") else "data"

        with util.named_cursor(cr, 20) as ncr:
            ncr.execute(
                """
                SELECT res_id, id, db_datas
                FROM ir_attachment
                WHERE res_model = 'spreadsheet.dashboard'
                AND res_field = %s
                """,
                [data_field],
            )
            for used_res_id, attachment_id, db_datas in ncr:
                if db_datas:
                    data = json.loads(db_datas.tobytes())
                    data, _ = modify_all_models(cr, data)
                    data, _ = modify_all_fields(cr, data)
                    write_attachment(cr, attachment_id, data)
    _logger.info("--- %s seconds to update dashboards ---" % (time.time() - start_time))

    # les putains de snapshots ..; eh ouiiii faut aussi supporter ça .. donc ca fait encore une saloperie a migrer en plus, TROP BIEN
    query = r"""
         SELECT res_id, id, db_datas
           FROM ir_attachment
          WHERE res_field = 'spreadsheet_snapshot'
        """

    # for unused_doc_id, attachment_id, db_datas in cr.fetchall():
    start_total_time = time.time()
    with util.named_cursor(cr, 20) as ncr:
        ncr.execute(query)

        for doc_id, attachment_id, db_datas in ncr:
            if db_datas:
                start_time = time.time()
                data = json.loads(db_datas.tobytes())
                data, _ = modify_all_fields(cr, data)
                data, _ = modify_all_models(cr, data)
                _logger.info("--- %s seconds to update ---" % (time.time() - start_time))
                write_attachment(cr, attachment_id, data)
                _logger.info("--- %s seconds to update  & write ---" % (time.time() - start_time))
    _logger.info("--- %s seconds to update snapshots ---" % (time.time() - start_total_time))

