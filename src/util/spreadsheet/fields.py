import collections
import re
from itertools import chain
from typing import Iterable

from odoo.osv import expression

from .data_wrappers import Spreadsheet, create_data_source_from_cmd
from .misc import (
    adapt_view_link_cells,
    remove_data_source_function,
    transform_data_source_functions,
)
from .revisions import CommandAdapter
from odoo.addons.base.maintenance.migrations import util
from odoo.upgrade.util.context import adapt_context, clean_context
from odoo.upgrade.util.domains import _adapt_one_domain


# stolen from util.fields:def remove_fields - probably need to extract/expose there
def remove_adapter(leaf, is_or, negated):
    # replace by TRUE_LEAF, unless negated or in a OR operation but not negated
    if is_or ^ negated:
        return [expression.FALSE_LEAF]
    return [expression.TRUE_LEAF]


def modify_all_fields(cr, data):
    spreadsheet = Spreadsheet(data)
    cells_adapters = ()
    revisions_adapters = ()  # commandAdapter tuple

    to_remove = collections.defaultdict(list)
    to_change = collections.defaultdict(dict)

    for model, fields in util.ENVIRON["__renamed_fields"].items():
        for old_value, new_value in fields.items():
            if not new_value:
                to_remove[model].append(old_value)
            else:
                to_change[model][old_value] = new_value

    # remove
    _remove_field_from_filter_matching(cr, spreadsheet, to_remove)
    x, y = _remove_field_from_list(cr, spreadsheet, to_remove)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_field_from_pivot(cr, spreadsheet, to_remove)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_field_from_graph(cr, spreadsheet, to_remove)
    cells_adapters += x
    revisions_adapters += y
    x, y = _remove_field_from_view_link(cr, spreadsheet, to_remove)
    cells_adapters += x
    revisions_adapters += y

    # rename
    x, y = _rename_field_in_list(cr, spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_field_in_pivot(cr, spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_field_in_chart(cr, spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_field_in_filters(cr, spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y
    x, y = _rename_field_in_view_link(cr, spreadsheet, to_change)
    cells_adapters += x
    revisions_adapters += y

    # spreadsheet.clean_empty_cells()  ## TODO remove, only do it once per data...

    return spreadsheet.data, cells_adapters, revisions_adapters


## RENAME
def _rename_function_fields(content, data_source_ids, functions, old, new):
    def adapter(fun_call):
        for arg in fun_call.args[1:]:
            if arg.value == old:
                arg.value = new
        return fun_call

    return transform_data_source_functions(content, data_source_ids, functions, adapter)


def _rename_field_in_chain(cr, model, field_model, field_chain, old, new) -> str:
    """Model on which the field chain refers to."""
    domain = [(field_chain, "=", 1)]
    domain = _adapt_one_domain(cr, field_model, old, new, model, domain)
    if domain is None:
        return field_chain
    return domain[0][0]


def _rename_field_in_list(cr, spreadsheet: Spreadsheet, fields_changes):
    list_ids = set()

    list_ids_per_model = collections.defaultdict(set)

    def rename(olist):
        for model in fields_changes:
            for old, new in fields_changes[model].items():
                _rename_data_source_field(cr, olist, model, old, new)
                if olist.model == model:
                    list_ids.add(olist.id)
                    list_ids_per_model[model].add(olist.id)
                    olist.fields = _rename_fields(old, new, olist.fields)

    for olist in spreadsheet.lists:
        rename(olist)

    def trucmachin(content):
        for model in list_ids_per_model:
            list_ids = list_ids_per_model[model]
            for old, new in fields_changes[model].items():
                content = _rename_function_fields(content, list_ids, {"ODOO.LIST", "ODOO.LIST.HEADER"}, old, new)
        return content

    # for cell in spreadsheet.cells:
    def update_cell_content(cell):
        cell["content"] = trucmachin(cell["content"])

    list_models = {olist.id: olist.model for olist in spreadsheet.lists}

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def rename_re_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        if fields := fields_changes.get(list_models[olist.id], {}):
            for old, new in fields.items():
                olist.fields = _rename_fields(old, new, olist.fields)

    def modify_cmd_content(cmd):
        if "content" in cmd:
            return dict(cmd, content=trucmachin(cmd.get("content")))
        else:
            return cmd

    return (lambda cell: update_cell_content(cell),), (
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("INSERT_ODOO_LIST", lambda cmd: rename(create_data_source_from_cmd(cmd))),
        CommandAdapter("RE_INSERT_ODOO_LIST", rename_re_insert),
        CommandAdapter("UPDATE_CELL", modify_cmd_content),
    )


def _rename_fields(old: str, new: str, fields: Iterable[str]) -> Iterable[str]:
    renamed = []
    for field in fields:
        if ":" in field:
            field, aggregate_operator = field.split(":")
            if field == old:
                renamed.append(new + ":" + aggregate_operator)
            else:
                renamed.append(field)
        elif field == old:
            renamed.append(new)
        else:
            renamed.append(field)
    return renamed


def _rename_field_in_pivot(cr, spreadsheet: Spreadsheet, fields_changes):
    pivot_ids = set()

    pivot_ids_per_model = collections.defaultdict(set)

    def rename(pivot):
        for model in fields_changes:
            for old, new in fields_changes[model].items():
                _rename_data_source_field(cr, pivot, model, old, new)
                if pivot.model == model:
                    pivot_ids.add(pivot.id)
                    pivot_ids_per_model[model].add(pivot.id)
                    pivot.col_group_by = _rename_fields(old, new, pivot.col_group_by)
                    pivot.row_group_by = _rename_fields(old, new, pivot.row_group_by)
                    pivot.measures = _rename_fields(old, new, pivot.measures)

    for pivot in spreadsheet.pivots:
        rename(pivot)

    def trucmachinList(content):
        for model in pivot_ids_per_model:
            list_ids = pivot_ids_per_model[model]
            for old, new in fields_changes[model].items():
                # TODORAR change function defition as we apply
                # this after the migration of revision,
                # so right now, odoo.pivot no longer exists ....
                content = _rename_function_fields(content, list_ids, {"ODOO.PIVOT", "ODOO.PIVOT.HEADER"}, old, new)
        return content

    # for cell in spreadsheet.cells:
    def update_cell_content(cell):
        cell["content"] = trucmachinList(cell["content"])

    pivot_models = {pivot.id: pivot.model for pivot in spreadsheet.pivots}

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if not pivot:
            return
        rename(pivot)
        pivot_models[pivot.id] = pivot.model
        adapt_pivot_table(cmd)

    def adapt_pivot_table(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if not pivot:
            return
        if fields := fields_changes.get(pivot_models[pivot.id], {}):
            for old, new in fields.items():
                table = cmd["table"]
                for row in table["cols"]:
                    for cell in row:
                        cell["fields"] = _rename_fields(old, new, cell["fields"])
                        # value can be the name of the measure (a field name)
                        cell["values"] = _rename_fields(old, new, cell["values"])
                for row in table["rows"]:
                    row["fields"] = _rename_fields(old, new, row["fields"])
                    row["values"] = _rename_fields(old, new, row["values"])
                cmd["table"]["measures"] = _rename_fields(old, new, table["measures"])
        return

    def modify_cmd_content(cmd):
        if "content" in cmd:
            return dict(cmd, content=trucmachinList(cmd.get("content")))
        else:
            return cmd

    return (lambda cell: update_cell_content(cell),), (
        CommandAdapter("INSERT_PIVOT", adapt_insert),
        CommandAdapter("RE_INSERT_PIVOT", adapt_pivot_table),
        CommandAdapter("UPDATE_CELL", modify_cmd_content),
    )


def _rename_data_source_field(cr, data_source, model, old, new):
    data_source.domain = (
        _adapt_one_domain(cr, model, old, new, data_source.model, data_source.domain) or data_source.domain
    )
    for measure in data_source.fields_matching.values():
        measure["chain"] = _rename_field_in_chain(cr, data_source.model, model, measure["chain"], old, new)
    if data_source.model == model:
        adapt_context(data_source.context, old, new)
        if data_source.order_by:
            data_source.order_by = _rename_order_by(data_source.order_by, old, new)


def _rename_order_by(order_by, old, new):
    if isinstance(order_by, list):
        return [_rename_order_by(order, old, new) for order in order_by]
    if order_by and order_by["field"] == old:
        order_by["field"] = new
    return order_by


def _rename_field_in_chart(cr, spreadsheet: Spreadsheet, fields_changes):
    def rename(chart):
        for model in fields_changes:
            for old, new in fields_changes[model].items():
                _rename_data_source_field(cr, chart, model, old, new)
                if chart.model == model:
                    if chart.measure == old:
                        chart.measure = new
                    chart.group_by = _rename_fields(old, new, chart.group_by)
                # return chart ## TODORAR investigate

    for chart in spreadsheet.odoo_charts:
        rename(chart)

    def adapt_create_chart(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            rename(chart)

    return (), (CommandAdapter("CREATE_CHART", adapt_create_chart),)


def _rename_field_in_filters(cr, spreadsheet: Spreadsheet, fields_changes):
    pivot_models = {pivot.id: pivot.model for pivot in spreadsheet.pivots}
    list_models = {olist.id: olist.model for olist in spreadsheet.lists}
    chart_models = {chart.id: chart.model for chart in spreadsheet.odoo_charts}

    def adapt_filter_per_ds(cmd, ds_type, ds_models):
        for ds_id, field in cmd[ds_type].items():
            ds_model = ds_models[ds_id]
            for model in fields_changes:
                for old, new in fields_changes[model].items():
                    field["chain"] = _rename_field_in_chain(cr, ds_model, model, field["chain"], old, new)

    def adapt_filter(cmd):
        adapt_filter_per_ds(cmd, "pivot", pivot_models)
        adapt_filter_per_ds(cmd, "list", list_models)
        adapt_filter_per_ds(cmd, "chart", chart_models)

    def collect_pivot(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if not pivot:
            return
        pivot_models[pivot.id] = pivot.model

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def collect_charts(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            chart_models[chart.id] = chart.model

    return (), (
        CommandAdapter("INSERT_PIVOT", collect_pivot),
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("CREATE_CHART", collect_charts),
        CommandAdapter("ADD_GLOBAL_FILTER", adapt_filter),
        CommandAdapter("EDIT_GLOBAL_FILTER", adapt_filter),
    )


def match_markdown_link(content):
    return re.match(r"\[.*\]\(odoo://view/(.*)\)", content)


def _rename_field_in_view_link(cr, spreadsheet: Spreadsheet, fields_changes):
    def adapt_view_link(action):
        model = action["modelName"]
        if fields := fields_changes.get(model, {}):
            for old, new in fields.items():
                domain = _adapt_one_domain(cr, model, old, new, model, action["domain"])
                if domain:
                    if isinstance(action["domain"], str):
                        domain = str(domain)
                    action["domain"] = domain
                adapt_context(action["context"], old, new)
        else:
            return

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


## Removal


def _remove_data_source_field(cr, data_source, models):
    model = data_source.model
    if fields := models.get(model, []):
        for field in fields:
            data_source.domain = (
                _adapt_one_domain(
                    cr, model, field, "ignored", data_source.model, data_source.domain, remove_adapter, force_adapt=True
                )
                or data_source.domain
            )

            adapt_context(data_source.context, field, "ignored")
            if data_source.order_by:
                data_source.order_by = _remove_order_by(data_source.order_by, field)


def _remove_order_by(order_by, field):
    if isinstance(order_by, list):
        return [order for order in order_by if order["field"] != field]
    if order_by and order_by["field"] == field:
        return None
    return order_by


def _remove_list_functions(content, list_ids, field):  # .????
    """Remove functions such as ODOO.LIST(1, 'field') or ODOO.LIST.HEADER(1, 'field')."""

    def filter_func(func_call_ast):
        return any(arg.value == field for arg in func_call_ast.args[1:])

    return remove_data_source_function(content, list_ids, {"ODOO.LIST", "ODOO.LIST.HEADER"}, filter_func)


def _remove_field_from_list(cr, spreadsheet: Spreadsheet, models):
    def _remove_field(olist):
        _remove_data_source_field(cr, olist, models)
        if removed_fields := models.get(olist.model, False):
            olist.fields = [column for column in olist.fields if column not in removed_fields]

    for olist in spreadsheet.lists:
        _remove_field(olist)

    # collect all list models inserted by INSERT_ODOO_LIST
    # because we need the models to adapt RE_INSERT_ODOO_LIST
    list_models = {olist.id: olist.model for olist in spreadsheet.lists}

    def collect_list(cmd):
        olist = create_data_source_from_cmd(cmd)
        list_models[olist.id] = olist.model

    def adapt_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        _remove_field(olist)

    def adapt_re_insert(cmd):
        olist = create_data_source_from_cmd(cmd)
        if models.get(list_models[olist.id], False):
            _remove_field(olist)


    return (), (
        CommandAdapter("INSERT_ODOO_LIST", collect_list),
        CommandAdapter("INSERT_ODOO_LIST", adapt_insert),
        CommandAdapter("RE_INSERT_ODOO_LIST", adapt_re_insert),
    )


def _remove_field_from_pivot(cr, spreadsheet: Spreadsheet, models):
    def _remove_field(pivot):
        _remove_data_source_field(cr, pivot, models)
        if removed_fields := models.get(pivot.model, False):
            pivot.col_group_by = [f for f in pivot.col_group_by if f not in removed_fields]
            pivot.row_group_by = [f for f in pivot.row_group_by if f not in removed_fields]
            pivot.measures = [f for f in pivot.measures if f not in removed_fields]

    for pivot in spreadsheet.pivots:
        _remove_field(pivot)

    def adapt_insert(cmd):
        pivot = create_data_source_from_cmd(cmd)
        if not pivot:
            return
        _remove_field(pivot)

    return (), (
        CommandAdapter("INSERT_PIVOT", adapt_insert),
        CommandAdapter("RE_INSERT_PIVOT", adapt_insert),
        # CommandAdapter("RE_INSERT_PIVOT", adapt_insert),
    )
    ## missing update pivot // ADD PIVOT etc
    ## will probably need to vberion the removefield stuff


def _remove_field_from_graph(cr, spreadsheet: Spreadsheet, models):
    def _remove_field(chart):
        _remove_data_source_field(cr, chart, models)
        if removed_fields := models.get(chart.model, False):
            chart.measure = chart.measure if chart.measure not in removed_fields else None

    for chart in spreadsheet.odoo_charts:
        _remove_field(chart)

    def adapt_create_chart(cmd):
        if cmd["definition"]["type"].startswith("odoo_"):
            chart = create_data_source_from_cmd(cmd)
            _remove_field(chart)

    return (), (CommandAdapter("CREATE_CHART", adapt_create_chart),)


def _remove_field_from_view_link(cr, spreadsheet: Spreadsheet, models):
    def adapt_view_link(action):
        model = action["modelName"]
        if fields := models.get(model, []):
            for field in fields:
                clean_context(action["context"], field)
                action["domain"] = (
                    _adapt_one_domain(
                        cr, model, field, "ignored", model, action["domain"], remove_adapter, force_adapt=True
                    )
                    or action["domain"]
                )

    return adapt_view_link_cells(spreadsheet, adapt_view_link)


def _remove_field_from_filter_matching(cr, spreadsheet: Spreadsheet, models):
    data_sources = chain(spreadsheet.lists, spreadsheet.pivots, spreadsheet.odoo_charts)
    for data_source in data_sources:
        matching_to_delete = []
        for filter_id, measure in data_source.fields_matching.items():
            for model_name in models:
                for field in models[model_name]:
                    if _is_field_in_chain(cr, model_name, field, data_source.model, measure["chain"]):
                        matching_to_delete.append(filter_id)
        for filter_id in matching_to_delete:
            del data_source.fields_matching[filter_id]


def _is_field_in_chain(cr, field_model, field, data_source_model, field_chain):
    def adapter(*args, **kwargs):
        return expression.FALSE_DOMAIN

    domain = [(field_chain, "=", 1)]
    domain = _adapt_one_domain(cr, field_model, field, "ignored", data_source_model, domain, adapter=adapter)
    return domain == expression.FALSE_DOMAIN


def domain_fields(domain):
    """Return all field names used in the domain
    >>> domain_fields([['field1', '=', 1], ['field2', '=', 2]])
    ['field1', 'field2'].
    """  # noqa: D205
    return [leaf[0] for leaf in domain if len(leaf) == 3]


def pivot_measure_fields(pivot):
    return [measure for measure in pivot.measures if measure != "__count"]


def pivot_fields(pivot):
    """Return all field names used in a pivot definition."""
    fields = set(pivot.col_group_by + pivot.row_group_by + pivot_measure_fields(pivot) + domain_fields(pivot.domain))
    measure = pivot.order_by and pivot.order_by["field"]
    if measure and measure != "__count":
        fields.add(measure)
    return fields


def chart_fields(chart):
    """Return all field names used in a chart definitions."""
    fields = set(chart.group_by + domain_fields(chart.domain))
    measure = chart.measure
    if measure != "__count":
        fields.add(measure)
    return fields


def list_order_fields(list_definition):
    return [order["field"] for order in list_definition.order_by]
