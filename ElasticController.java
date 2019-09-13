package com.example.elasticsearch;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.elasticsearch.model.TablesColumns;
import com.example.elasticsearch.model.TablesInfo;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class ElasticController {

	@Autowired
	private TransportClient client;

	// Method 1: Add records
	@PostMapping("/table/add")
	public ResponseEntity<String> add(@RequestBody TablesInfo tables) {
		try {

			XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
					.field("id", tables.getId())
					.field("XX", tables.getXX())
					.field("YY", tables.getYY())
					.field("ZZ", tables.getZZ());
			

			builder.endObject();
			IndexResponse response = this.client
					.prepareIndex("table".toLowerCase(), "tablestable", String.valueOf(tables.getId()))
					.setSource(builder).get();

			return new ResponseEntity<String>(response.getId(), HttpStatus.OK);

		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 2: Update record by using ID from table index
	@PostMapping("/table/{id}")
	public ResponseEntity<String> updateByIdFromES1(@PathVariable String id, String XX, String YY,
			String ZZ) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("XX", XX)
				.field("YY", YY)
				.field("ZZ", ZZ)
				.endObject();

		UpdateResponse response = this.client.prepareUpdate("table".toLowerCase(), "tablestable", id)
				.setDoc(builder).get();

		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}


//-----------------------------------------------------------------------------------------------------------------------		
	// Method 3: Delete record by using ID from table index
	@DeleteMapping("/table/{id}")
	public ResponseEntity<String> delete(@PathVariable String id) {

		DeleteResponse response = this.client.prepareDelete("table".toLowerCase(), "tablestable", id).get();
		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 4: Get All Records from table index
	@GetMapping("/table/all")
	public ResponseEntity<Map<String, Object>> getAll(
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|ASC") String sort) {
		
		System.out.println("String sort" + sort);
	
		if (page == null) {
			page = 0;
		}

		if (page >= 1) {
			page--;
		}
		
		Sort sort1 = new Sort(Sort.Direction.ASC, "id");
		Pageable pageable = new PageRequest(page, per_page,sort1 );
		List<TablesInfo> tables = new ArrayList<TablesInfo>();
		String[] parts = sort.split(Pattern.quote("|"));

		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;
			sort1 = new Sort(Sort.Direction.DESC, parts[0].trim());
			System.out.println(">>>>>>" +parts[0].trim());

		} else {
			sortOrder = SortOrder.ASC;
			sort1 = new Sort(Sort.Direction.ASC, parts[0].trim());
			System.out.println(">>>>>>" +parts[0].trim());
		}
		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		response = client.prepareSearch("table".toLowerCase()).setTypes("tablestable")
				.setQuery(QueryBuilders.matchAllQuery())
				.addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder)).setSize(pageable.getPageSize())
				.setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute().actionGet();
		for (SearchHit hit : response.getHits()) {
			TablesInfo table;
			try {

				table = mapper.readValue(hit.getSourceAsString(), TablesInfo.class);
				tables.add(table);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Page<TablesInfo> tableInfo = new PageImpl(tables, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructTablePagination(tableInfo, "/md_table_indx/all");
		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		
		System.out.println("content "+ tableInfo.getContent());
		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);
	}

	private Map<String, Object> constructTablePagination(Page<TablesInfo> pageT, String url) {
		System.out.println("getNumber: "+pageT.getNumber());
		System.out.println("getSize: "+pageT.getSize());
		
		Map<String, Object> paginationData = new HashMap<String, Object>();
		paginationData.put("total", pageT.getTotalElements());
		paginationData.put("per_page", pageT.getSize());
		paginationData.put("current_page", pageT.getNumber()+1);
		paginationData.put("last_page", pageT.getTotalPages());
		paginationData.put("next_page_url", PaginationUtil.generateUri(url, pageT.getNumber() + 1, pageT.getSize()));
		paginationData.put("prev_page_url", PaginationUtil.generateUri(url, pageT.getNumber() - 1, pageT.getSize()));
		paginationData.put("from", (pageT.getNumber()) * pageT.getSize());
		paginationData.put("to", (pageT.getSize() -1) + pageT.getSize() * (pageT.getNumber()));
		paginationData.put("nextpage", pageT.hasNextPage());
		paginationData.put("isFirstPage", pageT.isFirstPage());
		paginationData.put("isLastPage", pageT.isLastPage());
		return paginationData;
	}

//-----------------------------------------------------------------------------------------------------------------------			
//Method 5:Get records by filter (multiple) searching {searchKey} + ZZ from table index
	@GetMapping("/table/filterTableRecords/{searchKey}/{zz}")
	public ResponseEntity<String> filterColumnsRecords(@PathVariable Optional<String> searchKey,
			@PathVariable Optional<String> zz,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {

		Sort sort1 = null;

		if (page == null) {
			page = 0;
		}
		System.out.println("search_key:" + searchKey);

		System.out.println("SORT >>> " + sort1);

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Pageable pageable = new PageRequest(page, per_page, sort1);

		String[] zz = null;
		List<TablesInfo> tables = new ArrayList<TablesInfo>();
		String[] parts = sort.split(Pattern.quote("|"));
		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;

		} else {
			sortOrder = SortOrder.ASC;
		}

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

		if (dataDomain.isPresent()) {
			String[] tokens = dataDomain.get().toLowerCase().split(",");
			
			TermsQueryBuilder builder2 = QueryBuilders.termsQuery("ZZ", tokens);
			boolQueryBuilder.must(builder2);

		}
		if (searchKey.isPresent()) {

			  boolQueryBuilder.must(QueryBuilders.wildcardQuery("YY", "*" +
			  searchKey.get() + "*")); 
			  
			  MultiMatchQueryBuilder builder1 = QueryBuilders.multiMatchQuery(searchKey.get(), "YY",
			  "XX");
			  
			  builder1.analyzer("standard")
				  .field("YY",20)
				  .field("ZZ", 20)
				  .cutoffFrequency(0.001f)
				  .fuzziness(Fuzziness.AUTO)
				  .maxExpansions(100)
				  .prefixLength(10)
				  .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);
			  boolQueryBuilder.should(builder1);

		}

		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		response = client.prepareSearch("table".toLowerCase()).setTypes("tablestable")
				.setQuery(boolQueryBuilder).addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder))
				.setSize(pageable.getPageSize()).setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute()
				.actionGet();

		response.getHits().forEach(hit -> {
			String source = hit.getSourceAsString();
			try {
				TablesInfo table = mapper.readValue(source, TablesInfo.class);
				tables.add(table);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		Page<TablesInfo> tableInfo = new PageImpl(tables, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructTablePagination(tableInfo, "/table/filterTableRecords");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

	}

}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 6: Searching distinct zz value from table index
	@GetMapping("/table/zz")
	public List<String> getDistinctLabels() throws InterruptedException, ExecutionException {
		SearchRequestBuilder aggregationQuery = client.prepareSearch("table").setTypes("zz")
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("field").field("zz.keyword").size(100));

		SearchResponse response = aggregationQuery.execute().actionGet();
		Aggregation aggregation = response.getAggregations().get("field");
		StringTerms st = (StringTerms) aggregation;
		return st.getBuckets().stream().map(bucket -> bucket.getKeyAsString()).collect(toList());
	}
	
	

}
