# -*- coding: utf-8 -*-
import logging
import os
from collections import namedtuple

import lxml

from .exceptions import SleepyDeveloperError
from .misc import splitlines, version_between, version_gte

_logger = logging.getLogger(__name__.rpartition(".")[0])

_VALID_MODELS = frozenset(
    {
        "_unknown",
        "website_pricelist",
        "ir_actions_account_report_download",
        # see test_testing_utilities/models.py
        "o2m_readonly_subfield_parent",
        "o2m_readonly_subfield_child",
        "o2m_changes_parent",
        "o2m_changes_children",
    }
    | ({"l10n_pl_tax_office"} if version_between("16.0", "17.0") else set())
    | {m.strip() for m in os.getenv("UPG_VALID_MODELS", "").split(";")} - {""}
)

# python3 shims
try:
    unicode  # noqa: B018
except NameError:
    unicode = str


def table_of_model(cr, model):
    exceptions = dict(
        line.split()
        for line in splitlines(
            """
        ir.actions.actions          ir_actions
        ir.actions.act_url          ir_act_url
        ir.actions.act_window       ir_act_window
        ir.actions.act_window_close ir_actions
        ir.actions.act_window.view  ir_act_window_view
        ir.actions.client           ir_act_client
        ir.actions.report.xml       ir_act_report_xml
        ir.actions.report           ir_act_report_xml
        ir.actions.server           ir_act_server
        ir.actions.wizard           ir_act_wizard

        stock.picking.in  stock_picking
        stock.picking.out stock_picking

        workflow            wkf
        workflow.activity   wkf_activity
        workflow.instance   wkf_instance
        workflow.transition wkf_transition
        workflow.triggers   wkf_triggers
        workflow.workitem   wkf_workitem

        # mass_mailing
        mail.mass_mailing.list_contact_rel mail_mass_mailing_contact_list_rel
        mailing.contact.subscription       mailing_contact_list_rel

        # `mail.notification` was a "normal" model in versions <9.0
        # and a named m2m in >=saas~13
        # and renamed as a "normal" model table in >= saas~14.3
        {gte_saas13_lte_saas14_3} mail.notification mail_message_res_partner_needaction_rel

        project.task.stage.personal project_task_user_rel
    """.format(gte_saas13_lte_saas14_3="" if version_gte("9.saas~13") and not version_gte("saas~14.3") else "#")
        )
    )
    return exceptions.get(model, model.replace(".", "_"))


def model_of_table(cr, table):
    exceptions = dict(
        line.split()
        for line in splitlines(
            """
        # can also be act_window_close, but there are chances it wont be usefull for anyone...
        ir_actions         ir.actions.actions
        ir_act_url         ir.actions.act_url
        ir_act_window      ir.actions.act_window
        ir_act_window_view ir.actions.act_window.view
        ir_act_client      ir.actions.client
        ir_act_report_xml  {action_report_model}
        ir_act_server      ir.actions.server
        ir_act_wizard      ir.actions.wizard

        wkf            workflow
        wkf_activity   workflow.activity
        wkf_instance   workflow.instance
        wkf_transition workflow.transition
        wkf_triggers   workflow.triggers
        wkf_workitem   workflow.workitem

        _unknown  _unknown

        mail_mass_mailing_contact_list_rel mail.mass_mailing.list_contact_rel
        mailing_contact_list_rel           mailing.contact.subscription
        # Not a real model until saas~13
        {gte_saas13_lte_saas14_3} mail_message_res_partner_needaction_rel mail.notification

        project_task_user_rel project.task.stage.personal
    """.format(
                action_report_model="ir.actions.report" if version_gte("10.saas~17") else "ir.actions.report.xml",
                gte_saas13_lte_saas14_3="" if version_gte("9.saas~13") and not version_gte("saas~14.3") else "#",
            )
        )
    )
    try:
        return exceptions[table]
    except KeyError:
        cr.execute(
            """
            SELECT model
              FROM ir_model
             WHERE replace(model, '.', '_') = %s
            """,
            [table],
        )
        candidates = [m for (m,) in cr.fetchall()]
        if candidates:
            if len(candidates) > 1:
                _logger.critical("cannot determine model of table %r. Multiple candidates: %r", table, candidates)
            return candidates[0]

        fallback = table.replace("_", ".")
        _logger.critical(
            "cannot determine model of table %r. No candidates found in the `ir_model` table. Fallback to %r",
            table,
            fallback,
        )
        return fallback


def _validate_model(model):
    if "_" in model and "." not in model and not model.startswith("x_") and model not in _VALID_MODELS:
        raise SleepyDeveloperError("`{}` seems to be a table name instead of model name".format(model))
    return model


def _validate_table(table):
    if "." in table:
        raise SleepyDeveloperError("`{}` seems to be a model name instead of table name".format(table))
    return table


def _ir_values_value(cr, prefix=None):
    # returns the casting from bytea to text needed in saas~17 for column `value` of `ir_values`
    # returns tuple(column_read, cast_write)
    cache = getattr(_ir_values_value, "cache", None)

    if cache is None:
        from .pg import column_type

        if column_type(cr, "ir_values", "value") == "bytea":
            cr.execute("SELECT character_set_name FROM information_schema.character_sets")
            (charset,) = cr.fetchone()
            column_read = "convert_from(%%svalue, '%s')" % charset
            cast_write = "convert_to(%%s, '%s')" % charset
        else:
            column_read = "%svalue"
            cast_write = "%s"
        _ir_values_value.cache = (column_read, cast_write)
    else:
        column_read, cast_write = cache

    prefix = prefix + "." if prefix else ""
    return column_read % prefix, cast_write


def _dashboard_actions(cr, arch_match, *models):
    """Yield (dashboard_id, action) of dashboards that match `arch_match` and apply on `models` (if specified)."""
    q = """
        SELECT id, arch
          FROM ir_ui_view_custom
         WHERE arch ~ %s
    """
    cr.execute(q, [arch_match])
    for dash_id, arch in cr.fetchall():
        try:
            if isinstance(arch, unicode):
                arch = arch.encode("utf-8")  # noqa: PLW2901
            dash = lxml.etree.fromstring(arch)
        except lxml.etree.XMLSyntaxError:
            _logger.exception("Cannot parse dashboard %s", dash_id)
            continue
        for act in dash.xpath("//action"):
            if models:
                try:
                    act_id = int(act.get("name", "FAIL"))
                except ValueError:
                    continue

                cr.execute("SELECT res_model FROM ir_act_window WHERE id = %s", [act_id])
                [act_model] = cr.fetchone() or [None]
                if act_model not in models:
                    continue
            yield dash_id, act

        cr.execute(
            "UPDATE ir_ui_view_custom SET arch = %s WHERE id = %s",
            [lxml.etree.tostring(dash, encoding="unicode"), dash_id],
        )


def _get_theme_models():
    return {
        "theme.ir.ui.view": "ir.ui.view",
        "theme.ir.asset": "ir.asset",
        "theme.website.page": "website.page",
        "theme.website.menu": "website.menu",
        "theme.ir.attachment": "ir.attachment",
    }


FieldsPathPart = namedtuple(
    "FieldsPathPart",
    "model path part_index field_model field_name relation_model",
)


def _resolve_model_fields_path(cr, model, path):
    """
    Resolve model fields paths (e.g. `hr.appraisal` `['employee_id', 'user_id', 'partner_id']`).

    :param str model: the model to resolve the fields ``path`` from.
    :param typing.Sequence[str] path: the fields path starting from ``model``.
    :return: a list of the resolved fields path parts through their relation models.
    :rtype: list[FieldsPathPart]

    :meta private: exclude from online docs
    """
    path = list(path)
    cr.execute(
        """
        WITH RECURSIVE resolved_fields_path AS (
            -- non-recursive term
               SELECT p.model                       AS model,
                      p.path                        AS path,
                      1                             AS part_index,
                      p.model                       AS field_model,
                      p.path[1]                     AS field_name,
                      imf.relation                  AS relation_model
                 FROM (VALUES (%(model)s, %(path)s)) p(model, path)
            LEFT JOIN ir_model_fields imf
                   ON imf.model = p.model
                  AND imf.name = p.path[1]

            UNION ALL

            -- recursive term
               SELECT rfp.model,
                      rfp.path,
                      rfp.part_index + 1            AS part_index,
                      rfp.relation_model            AS field_model,
                      rfp.path[rfp.part_index + 1]  AS field_name,
                      rimf.relation                 AS relation_model
                 FROM resolved_fields_path rfp
            LEFT JOIN ir_model_fields rimf
                   ON rimf.model = rfp.relation_model
                  AND rimf.name = rfp.path[rfp.part_index + 1]
                WHERE cardinality(rfp.path) > rfp.part_index
                  AND rfp.relation_model IS NOT NULL
        )
        SELECT * FROM resolved_fields_path
        ORDER BY model, path, part_index
        """,
        {"model": model, "path": list(path)},
    )
    return [FieldsPathPart(**row) for row in cr.dictfetchall()]
