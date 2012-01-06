import re
import copy
import time
import urllib
from datetime import datetime, timedelta
from trac.core import *
from trac.config import Option, ListOption
from trac.ticket import TicketSystem, Milestone
from trac.ticket.roadmap import TicketGroupStats, ITicketGroupStatsProvider
from trac.ticket.roadmap import DefaultTicketGroupStatsProvider
from trac.web import IRequestHandler, IRequestFilter
from trac.web.chrome import ITemplateProvider, add_stylesheet
from trac.util import as_bool
from trac.util.translation import _

# FIXME: sql injection

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
    filter = ListOption('sumstats', 'filter', [],
        _("Filters out tickets to sum (e.g, type!=epic)."))
    
    
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
        the specified field and its value."""
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
        the field values."""
        if ticket_ids is None:
            return 0.0
        
        name = field['name']
        sum_field = self.sum_field and self._get_field(self.sum_field) or None
        
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        sql = "SELECT "
        id_list = ",".join([str(x) for x in sorted(ticket_ids)])
        
        # handle count vs. sum
        if sum_field:
            if 'custom' in sum_field:
                sql += "SUM(sf.value) "
            else:
                sql += "SUM(%s) " % self.sum_field
        else:
            sql += "COUNT(*) "
        sql += "FROM ticket t "
        
        # add sum field join
        if sum_field and 'custom' in sum_field:
            sql += "LEFT OUTER JOIN ticket_custom sf ON sf.ticket = t.id" +\
                     " AND sf.name='%s' " % self.sum_field
        
        # handle built-in vs. custom field
        value = group.get('status','')
        vals = ','.join(["'%s'" % v.strip() for v in value.split(',')])
        if 'custom' in field:
            sql += "LEFT OUTER JOIN ticket_custom ff ON ff.ticket = t.id" +\
                     " AND ff.name='%s' WHERE ff.value IN (%s) " % (name,vals)
        else:
            sql += "WHERE t.%s IN (%s) " % (name,vals)
        
        # apply the filter (if any)
        for filter in self.filter:
            fld,val = filter.split('=',1)
            if fld.endswith('!'):
                sql += "AND t.%s != '%s' " % (fld[:-1],val)
            else:
                sql += "AND t.%s = '%s' " % (fld,val)
        
        # assume only open tickets
        if name not in ('status','resolution'):
            sql += "AND t.status != 'closed' "
            group.get('query_args').setdefault('status', []).append('!closed')
        
        sql += "AND t.id IN (%s);" % id_list
        cursor.execute(sql)
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


class SumTicketDataSourceProvider(Component):
    implements(IRequestHandler)
    
    sum_field = Option('sumstats', 'field', '', _("Field name to sum."))
    filter = ListOption('sumstats', 'filter', '',
        _("Filters out tickets to sum (e.g, type!=epic)."))
    
    # IRequestHandler methods
    def match_request(self, req):
        return req.path_info.startswith('/sumstats/query') and \
               self._get_milestone(req)

    def process_request(self, req):
        """Process data source request.  The one param is grabbed from the
        referral url instead of as a param.
        """
        try:
            columns = self._get_columns(req)
            milestone = self._get_milestone(req)
            data = self._get_burndown(columns, milestone)
            code,type,msg = 200,'application/json',data
        except Exception, e:
            import traceback;
            code,type = 500,'text/plain'
            msg = "Oops...\n"+traceback.format_exc()+"\n"
            self.log.error(msg)
        req.send_response(code)
        req.send_header('Content-Type', type)
        req.send_header('Content-Length', len(msg))
        req.end_headers()
        req.write(msg)
    
    # private methods
    def _get_columns(self, req):
        """Extract which columns and their order from the query string.
        This enables alternative chart configurations.  Note that the
        'day' will remain as the first column always."""
        columns = []
        tq = req.args.get('tq')
        if tq:
            select_re = re.compile(r"select (?P<columns>.+)( from)?")
            match = select_re.search(tq)
            if match:
                cols = match.groupdict()['columns']
                for col in cols.split(','):
                    columns.append(col.strip())
            else:
                raise Exception("Unable to parse query %s" % tq)
        else:
            columns = ['todo','done']
        return columns
    
    def _get_milestone(self, req):
        """Extract the milestone from the referer url.  If not found then
        return the current milestone."""
        path = req.environ.get('HTTP_REFERER','')
        milestone_re = re.compile(r"/milestone/(?P<milestone>[^?]+)")
        match = milestone_re.search(path)
        if match:
            name = urllib.unquote(match.groupdict()['milestone'])
            for m in Milestone.select(self.env, include_completed=True):
                if m.name == name:
                    return m
            else:
                raise Exception("Milestone %s not found" % name)
        else:
            # milestone not found in url, so assume current milestone
            for m in Milestone.select(self.env, include_completed=False):
                return m
            else:
                raise Exception("No provided or current milestone")
        return None
    
    def _get_start_date(self, milestone):
        """Returns the start date for the given milestone based on the
        prior milestone as follows - this milestone's start date =
        
         * the day after the prior milestone's completed date (if completed)
         * else the day after the prior milestone's due date (if due)
         * else today
        """
        prior = None
        for m in Milestone.select(self.env, include_completed=True):
            if m.name == milestone.name:
                break
            prior = m
        else:
            raise Exception("Milestone %s not found" % milestone.name)
        
        if prior:
            if prior.completed:
                return self._get_day(prior.completed, 'next')
            if prior.due:
                return self._get_day(prior.due, 'next')
        return self._get_day(datetime.utcnow(), 'end')
    
    def _get_end_date(self, milestone, start_date):
        """Returns the end date to use for the burndown chart based on the
        given milestone.
        """
        if milestone.completed:
            return self._get_day(milestone.completed, 'end')
        today = self._get_day(datetime.utcnow(), 'end')
        if milestone.due and today > self._get_day(milestone.due, 'end'):
            return today
        if today > start_date:
            return today
        return self._get_day(start_date, 'end')
    
    def _get_day(self, date, directive):
        """Returns a time of day UTC based on the given directive of:
        
         * start - midnight
         * end - one microsecond before midnight
         * next - midnight of next day
        """
        # midnight UTC
        day = datetime(date.year, date.month, date.day, 0, 0, 0, 0, None)
        if directive == 'start':
            return day
        
        # midnight the next day UTC
        day += timedelta(days=1); # add a day
        if directive == 'next':
            return day
        
        # one second before the next day UTC (end of day)
        return day - timedelta(0, seconds=1)
        
    def _get_burndown(self, columns, milestone):
        """Return data for a burndown chart for the given columns."""
        start_date = self._get_start_date(milestone)
        end_date = self._get_end_date(milestone, start_date)
        
        # find a nice buffer
        today = self._get_day(datetime.utcnow(), 'end').replace(tzinfo=None)
        if today < start_date:
            buffer = start_date
        else:
            buffer = today
        buffer += timedelta(days=7)
        if milestone.completed:
            date = milestone.completed.replace(tzinfo=None)
            if date < buffer:
                buffer = date
        elif milestone.due:
            date = milestone.due.replace(tzinfo=None)
            if date < today:
                buffer = today
            elif date < buffer:
                buffer = date
        
        # fetch the data for each day
        rows = []
        day = self._get_day(start_date, 'end') # include whole day
        while day <= buffer:
            if day > end_date:
                total,done,todo = 0.0,0.0,0.0
            else:
                total,done,todo = self._get_burndown_day(milestone, day)
            # add to rows
            rows.append({'day':day, 'total':total, 'todo':todo, 'done':done})
            day += timedelta(days=1)
        
        # package the data for the google visualization query response
        import gviz_api
        schema = {"day": ("date", "Day"),
                  "total": ("number", "Total"),
                  "todo": ("number", "To Do"),
                  "done": ("number", "Done")}
        data = gviz_api.DataTable(schema)
        data.LoadData(rows)
        return data.ToJSonResponse(
            columns_order=("day",) + tuple(columns),
            order_by="day")
    
    def _get_burndown_day(self, milestone, day):
        """Return the total work, work done and left to do on a given day."""
        ms = long(time.mktime(day.utctimetuple()))*long(1000000)
        
        # decide to sum or count
        if self.sum_field:
            sum = "COALESCE(tc1.newvalue,e.value)"
        else:
            sum = "1" # equivalent to COUNT(*)
        
        sql = """
        SELECT SUM(%(sum)s) AS "Total",
               SUM(CASE COALESCE(tc5.newvalue,'new')
                   WHEN 'closed' THEN %(sum)s
                   ELSE 0 END) AS "Done",
               SUM(CASE COALESCE(tc5.newvalue,'new')
                   WHEN 'closed' THEN 0
                   ELSE %(sum)s END) AS "To Do"
        FROM ticket t
        LEFT OUTER JOIN ticket_change tc1 ON tc1.ticket=t.id AND
                          tc1.field='%(field)s' AND tc1.time=
                          (SELECT max(tc2.time) FROM ticket_change tc2
                           WHERE tc2.ticket=t.id AND tc2.field='%(field)s'
                            AND tc2.time<=%(day)s)
        LEFT OUTER JOIN ticket_change tc3 ON tc3.ticket = t.id AND
                          tc3.field='milestone' AND tc3.time=
                          (SELECT max(tc4.time) FROM ticket_change tc4
                           WHERE tc4.ticket=t.id AND tc4.field='milestone'
                            AND tc4.time<=%(day)s)
        LEFT OUTER JOIN ticket_change tc5 ON tc5.ticket = t.id AND
                          tc5.field='status' AND tc5.time=
                          (SELECT max(tc6.time) FROM ticket_change tc6
                           WHERE tc6.ticket=t.id AND tc6.field='status'
                            AND tc6.time<=%(day)s)
        LEFT OUTER JOIN ticket_change tc7 ON tc7.ticket = t.id AND
                          tc7.field='resolution' AND tc7.time=
                          (SELECT max(tc8.time) FROM ticket_change tc8
                           WHERE tc8.ticket=t.id AND tc8.field='resolution'
                            AND tc8.time<=%(day)s)
        LEFT OUTER JOIN ticket_custom e ON e.ticket=t.id AND e.name='%(field)s'
        WHERE (COALESCE(tc5.newvalue,'new')!='closed'
               OR COALESCE(tc7.newvalue,'')='fixed')
         AND t.time <= %(day)s
         AND COALESCE(tc3.newvalue,t.milestone)='%(milestone)s' 
        """ % {'sum':sum,'field':self.sum_field,
               'day':ms,'milestone':milestone.name}
        #self.log.debug("burndown day %s:%s" % (str(day),sql))
        
        # apply the filter (if any)
        for filter in self.filter:
            fld,val = filter.split('=',1)
            if fld.endswith('!'):
                sql += "AND t.%s != '%s' " % (fld[:-1],val)
            else:
                sql += "AND t.%s = '%s' " % (fld,val)
        
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        cursor.execute(sql)
        for total,done,todo in cursor:
            return float(total or 0.0),float(done or 0.0),float(todo or 0.0)
        return 0.0,0.0,0,0
