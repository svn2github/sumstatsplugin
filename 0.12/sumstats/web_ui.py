import re
import copy
from trac.core import *
from trac.config import Option
from trac.ticket import TicketSystem
from trac.ticket.roadmap import TicketGroupStats, ITicketGroupStatsProvider, DefaultTicketGroupStatsProvider
from trac.web import IRequestHandler, IRequestFilter
from trac.web.chrome import ITemplateProvider, add_stylesheet
from trac.util import as_bool
from trac.util.translation import _


class SumTicketGroupStatsProvider(DefaultTicketGroupStatsProvider):
    
    implements(ITicketGroupStatsProvider,
               IRequestFilter, IRequestHandler, ITemplateProvider)

    # trac.ini options
    sum_field = Option('sumstats', 'field', '', _("Field name to sum."))
    sum_label = Option('sumstats', 'label', _('tickets'),
                    _("Plural name of the items being summed."))
    drilldown_label = Option('sumstats', 'drilldown_label', _('Ticket status'),
                    _("Name of the milestone drilldown label."))
    query_args = Option('sumstats', 'query_args', '',
                    _("Comma-delimited args to add to all queries."))

    
    def _get_groups(self, ticket_ids=None):
        """Return a set of all ticket group names in order."""
        groups = []
        for group in copy.deepcopy(self._get_ticket_groups()):
            self._update_group(group, ticket_ids)
            groups.append(group)
        return groups
    
    def _update_group(self, group, ticket_ids):
        """Update the given group dict extracted from the [milestone-groups]
        section.  Also, a total count for the group is determined based on
        the specified field and its value.
        """
        field = self._get_field(group.get('field','status'))
        if group.get('status') == '*':
            group['status'] = self._get_remaining_values(group, field)
        group['query_args'] = self._get_query_args(group)
        group['total'] = self._get_total(group, field, ticket_ids)
    
    def _get_field(self, field_name):
        """Returns the ticket field corresponding to the given group."""
        for field in TicketSystem(self.env).get_ticket_fields():
            if field['name'] == field_name:
                return field
        raise Exception("Ticket field '%s' not found." % field_name)
    
    def _get_remaining_values(self, group, field):
        """Returns a comma-delimited string of all remaining values for
        a 'catch all' value of '*'."""
        remaining_values = set(field.get('options'))
        for grp in self._get_ticket_groups():
            if grp.get('field','status') != group.get('field','status'):
                continue
            value = grp.get('status')
            if value == '*':
                continue # TODO: raise if more than one '*'
            # TODO: raise if reused a value, or if no remaining values
            remaining_values -= set([v.strip() for v in value.split(',')])
        return ','.join(remaining_values)
    
    def _get_query_args(self, group):
        query_args = {}
        value = group.get('status')
        for v in value.split(','):
            query_args.setdefault(group.get('field','status'), []).append(v)
        args = group.get('query_args','').split(',')+self.query_args.split(',')
        for arg in [kv for kv in args if '=' in kv]:
            k, v = [a.strip() for a in arg.split('=', 1)]
            query_args.setdefault(k, []).append(v)
        return query_args
        
    
    def _get_total(self, group, field, ticket_ids):
        """Return either the total (a) count of tickets, or (b) sum of
        the field values (if the group specifies )"""
        if ticket_ids is None:
            return 0.0
        
        name = field['name']
        sum_field = self.sum_field and self._get_field(self.sum_field) or None
        
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        query = "SELECT "
        id_list = ",".join([str(x) for x in sorted(ticket_ids)])
        
        # handle count vs. sum
        if sum_field:
            if 'custom' in sum_field:
                query += "SUM(sf.value) "
            else:
                query += "SUM(%s) " % self.sum_field
        else:
            query += "COUNT(*) "
        query += "FROM ticket t "
        
        # add sum field join
        if sum_field:
            query += "LEFT OUTER JOIN ticket_custom sf ON sf.ticket = t.id" +\
                     " AND sf.name='%s' " % self.sum_field
        
        # handle built-in vs. custom field
        value = group.get('status','')
        vals = ','.join(["'%s'" % v.strip() for v in value.split(',')])
        if 'custom' in field:
            query += "LEFT OUTER JOIN ticket_custom ff ON ff.ticket = t.id" +\
                     " AND ff.name='%s' WHERE ff.value IN (%s) " % (name,vals)
        else:
            query += "WHERE t.%s IN (%s) " % (name,vals)
        
        # assume only open tickets
        if name not in ('status','resolution'):
            query += "AND t.status != 'closed' "
            group.get('query_args').setdefault('status', []).append('!closed')
        
        query += "AND t.id IN (%s);" % id_list
        cursor.execute(query)
        for (total,) in cursor:
            return float(total or 0.0)
        return 0.0
    
    
    # ITicketGroupStatsProvider methods
    def get_ticket_group_stats(self, ticket_ids):
        stat = TicketGroupStats(self.drilldown_label, self.sum_label)
        for group in self._get_groups(ticket_ids):
            stat.add_interval(
                title=group.get('label', group['name']),
                count=group.get('total', 0),
                qry_args=group.get('query_args', {}),
                css_class=group.get('css_class', group['name']),
                overall_completion=as_bool(group.get('overall_completion')))
        stat.refresh_calcs()
        return stat

    # IRequestFilter methods
    def pre_process_request(self, req, handler):
        return handler

    def post_process_request(self, req, template, data, content_type):
        if req.path_info == '/sumstats/sumstats.css':
            return template, data, 'text/css'
        if 'milestone-groups' in self.config and \
           (re.match(r'/roadmap/?', req.path_info)
            or re.match(r'/milestone/.*', req.path_info)):
            add_stylesheet(req, '/sumstats/sumstats.css')
        return template, data, content_type

    # IRequestHandler methods
    def match_request(self, req):
        return req.path_info == '/sumstats/sumstats.css'

    def process_request(self, req):
        data = {'groups': self._get_groups()}
        return 'sumstats.css', data, 'text/css'

    # ITemplateProvider methods
    def get_htdocs_dirs(self):
        return []

    def get_templates_dirs(self):
        from pkg_resources import resource_filename
        return [resource_filename(__name__, 'templates')]
