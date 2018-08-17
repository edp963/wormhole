package nl.anchormen.sql4es.parse.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingElement;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.model.Column.Operation;

/**
 * A Presto {@link AstVisitor} implementation that parses GROUP BY clauses
 * 
 * @author cversloot
 *
 */
public class GroupParser extends SelectParser {
	
	public TermsBuilder parse(List<GroupingElement> elements, QueryState state){
		List<Column> groups = new ArrayList<Column>();
		for(GroupingElement grouping : elements){
			for(Set<Expression> expressions : grouping.enumerateGroupingSets()){
				for(Expression e : expressions)	groups.add((Column) e.accept(this, state) );
			}
		}
		
		// to find case sensitive group by definitions which ES needs
		for(Column groupby : groups){
			if(groupby.getOp() != Operation.NONE){
				state.addException("Can not use function '"+groupby.getAggName()+"' as GROUP BY, please use an alias to group by a function");
				return null;
			}
		}
		Heading.fixColumnReferences(state.originalSql()+";", "group by.+", "\\W", groups);
		
		for(Column g : groups){
			Column s = state.getHeading().getColumnByLabel(g.getAggName());
			if(s == null ){
				state.addException("Group by '"+g.getColumn()+"' not defined in SELECT");
			}else{
				// add column from select to this group (when referenced through an alias)
				g.setColumn(s.getColumn());
			}
		}
		return buildAggregationQuery(groups, 0, state);
	}
	
	/**
	 * Adds aggregations recursively
	 * All metric columns are added to last aggregation
	 * @param aggs
	 * @param index
//	 * @param metrics
	 * @return
	 */
	private TermsBuilder buildAggregationQuery(List<Column> aggs, int index, QueryState state){
		Column agg = aggs.get(index);
		TermsBuilder result = null;
		if(aggs.get(index).getOp() == Operation.NONE){
			result = AggregationBuilders.terms(agg.getAggName()).field(agg.getColumn());
			result.size(state.getIntProp(Utils.PROP_FETCH_SIZE, 10000));
		}
		if(index < aggs.size() - 1) result.subAggregation(buildAggregationQuery(aggs, index+1, state));
		else addMetrics(result, state.getHeading(), true); 
		return result;
	}
	
	/**
	 * Adds a Filtered Aggregation used to aggregate all results for a query without having a Group By
	 */
	public FilterAggregationBuilder buildFilterAggregation(QueryBuilder query, Heading heading){
		FilterAggregationBuilder filterAgg = AggregationBuilders.filter("filter").filter(query);
		addMetrics(filterAgg, heading, false);
		return filterAgg;
	}
	
	/**
	 * Adds a set of 'leaf aggregations' to the provided parent metric (i.e. count, sum, max etc)
	 * @param parentAgg
//	 * @param metrics
	 * @param addCount
	 */
	@SuppressWarnings("rawtypes")
	private void addMetrics(AggregationBuilder parentAgg, Heading heading, boolean addCount){
		for(Column metric : heading.columns()){
			if(metric.getOp() == Operation.AVG) 
				parentAgg.subAggregation(AggregationBuilders.avg(metric.getAggName()).field(metric.getColumn()));
			else if(addCount && metric.getOp() == Operation.COUNT)
				parentAgg.subAggregation(AggregationBuilders.count(metric.getAggName()));
			else if(metric.getOp() == Operation.MAX) 
				parentAgg.subAggregation(AggregationBuilders.max(metric.getAggName()).field(metric.getColumn()));
			else if(metric.getOp() == Operation.MIN) 
				parentAgg.subAggregation(AggregationBuilders.min(metric.getAggName()).field(metric.getColumn()));
			else if(metric.getOp() == Operation.SUM) 
				parentAgg.subAggregation(AggregationBuilders.sum(metric.getAggName()).field(metric.getColumn()));
		}
	}
	
	public TermsBuilder addDistinctAggregation(QueryState state){
		List<Column> distinct = new ArrayList<Column>();
		for(Column s : state.getHeading().columns()){
			if(s.getOp() == Operation.NONE && s.getCalculation() == null) distinct.add(s);
		}
		return buildAggregationQuery(distinct, 0, state);
	}
	
	public FilterAggregationBuilder addCountDistinctAggregation(QueryState state){
		FilterAggregationBuilder result = AggregationBuilders.filter("cardinality_aggs").filter(QueryBuilders.matchAllQuery());
		for(Column col : state.getHeading().columns()){
			result.subAggregation( AggregationBuilders.cardinality(col.getLabel()).field(col.getColumn()).precisionThreshold(state.getIntProp(Utils.PROP_PRECISION_THRESHOLD, 3000)) );
		}
		return result;
	}
	
}
