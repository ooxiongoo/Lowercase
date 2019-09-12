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

	// Method 1: Add and Update record from md_table_indx
	@PostMapping("/md_table_indx/add")
	public ResponseEntity<String> add(@RequestBody TablesInfo tables) {
		try {

			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", tables.getId())
					.field("Storage Type", tables.getStorage_type()).field("Source System", tables.getSource_system())
					.field("Schema", tables.getSchema()).field("Table Name", tables.getTable_name())
					.field("Table Description", tables.getTable_desc()).field("Schedule", tables.getSchedule())
					.field("Row Count", tables.getRow_count()).field("Data As Of Date", tables.getData_as_of_date())
					.field("Load Date", tables.getLoad_date()).field("Data Domain", tables.getData_domain());
			if (tables.getData_domain() != null) {
				builder.startArray("Data Domain");
				for (String str : tables.getData_domain()) {
					builder.value(str);

				}
				builder.endArray();
			}

			builder.endObject();
			IndexResponse response = this.client
					.prepareIndex("md_table_indx".toLowerCase(), "tablestable", String.valueOf(tables.getId()))
					.setSource(builder).get();

			return new ResponseEntity<String>(response.getId(), HttpStatus.OK);

		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	// Add and Update record from md_attributes_indx
	@PostMapping(value = "/md_attributes_indx/add")
	public ResponseEntity<String> addAttributes(@RequestBody TablesColumns columns) {

		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", columns.getId())
					.field("Table Name", columns.getTable_name()).field("Column Name", columns.getColumn_name())
					.field("Data Type", columns.getData_type())
					.field("Business Description", columns.getBus_description())
					.field("Data Domain", columns.getData_domain()).endObject();

			IndexResponse response = this.client
					.prepareIndex("md_attributes_indx".toLowerCase(), "tablescolumn", String.valueOf(columns.getId()))
					.setSource(builder).get();

			return new ResponseEntity<String>(response.getId(), HttpStatus.OK);

		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 2: Get Records By Searching Id from md_table_indx
	@GetMapping("/md_table_indx/{id}")
	public ResponseEntity<Map<String, Object>> getByIdFromES(@PathVariable int id) {

		GetResponse response = this.client.prepareGet("md_table_indx".toLowerCase(), "tablestable", String.valueOf(id))
				.get();
		if (!response.isExists()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Map<String, Object>>(response.getSource(), HttpStatus.OK);
	}

	// Get Records By Searching Id from md_attributes_indx
	@GetMapping("/md_attributes_indx/{id}")
	public ResponseEntity<Map<String, Object>> getByIdFromESAttributes(@PathVariable int id) {

		GetResponse response = this.client.prepareGet("md_attributes_indx".toLowerCase(), "tablescolumn", String.valueOf(id))
				.get();
		if (!response.isExists()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Map<String, Object>>(response.getSource(), HttpStatus.OK);
	}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 3: Update record by using ID from md_table_indx
	@PostMapping("/md_table_indx/{id}")
	public ResponseEntity<String> updateByIdFromES1(@PathVariable String id, String storage_type, String source_system,
			String schema, String table_name, String table_desc, String schedule, String row_count,
			String data_as_of_date, String load_date, String data_domain) throws IOException {

		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("Storage Type", storage_type)
				.field("Source System", source_system).field("schema", schema).field("Table Name", table_name)
				.field("Table Description", table_desc).field("Schedule", schedule).field("Row Count", row_count)
				.field("Data As Of Date", data_as_of_date).field("Load Date", load_date).field("Data Domain", data_domain)
				.endObject();

		UpdateResponse response = this.client.prepareUpdate("md_table_indx".toLowerCase(), "tablestable", id)
				.setDoc(builder).get();

		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}

	// Method 3: Update record by using ID from md_attributes_indx
	@PostMapping("/md_attributes_indx/{id}")
	public ResponseEntity<String> updateByIdFromES1Attributes(@PathVariable String id, String table_name, 
			String column_name, String data_type, String bus_description, String data_domain) throws IOException {
//		@RequestBody TablesColumns columns
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
				.field("Table Name", table_name)
				.field("Column Name", column_name)
				.field("Data Type", data_type)
				.field("Business Description", bus_description)
				.field("Data Domain",data_domain).endObject();

//		.field("Table Name", columns.getTable_name())
//		.field("Column Name", columns.getColumn_name())
//		.field("Data Type", columns.getData_type())
//		.field("Business Description", columns.getBus_description())
//		.field("Data Domain", columns.getData_domain()).endObject();
		
		UpdateResponse response = this.client
				.prepareUpdate("md_attributes_indx".toLowerCase(), "tablescolumn", id)
				.setDoc(builder).get();  
//		String.valueOf(columns.getId())

		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}

//-----------------------------------------------------------------------------------------------------------------------		
	// Method 4: Delete record by using ID from md_table_indx
	@DeleteMapping("/md_table_indx/{id}")
	public ResponseEntity<String> delete(@PathVariable String id) {

		DeleteResponse response = this.client.prepareDelete("md_table_indx".toLowerCase(), "tablestable", id).get();
		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}

	// Method 4: Delete records by using ID from md_attributes_indx
	@DeleteMapping("/md_attributes_indx/{id}")
	public ResponseEntity<String> deleteColumns(@PathVariable String id) {

		DeleteResponse response = this.client.prepareDelete("md_attributes_indx".toLowerCase(), "tablescolumn", id)
				.get();
		return new ResponseEntity<String>(response.getResult().toString(), HttpStatus.OK);
	}

//-----------------------------------------------------------------------------------------------------------------------	
	// Method 5: Get All Records from md_table_indx
	@GetMapping("/md_table_indx/all")
	public ResponseEntity<Map<String, Object>> getAll(
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|ASC") String sort) {
		
		System.out.println("String sort" + sort);
		//Sort sort1 = null;

		//Sort sort1 = new Sort(Sort.Direction.DESC, "storage_type","source_system","schema","table_desc","row_count","data_as_of_date","load_date");

		if (page == null) {
			page = 0;
		}

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
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
		response = client.prepareSearch("md_table_indx".toLowerCase()).setTypes("tablestable")
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

		// return return_object;
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

	@GetMapping("/md_attributes_indx/all")
	public ResponseEntity<String> getAtt_All(
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort" , defaultValue = "id|ASC") String sort) {
		
		//Sort sort1 = null;
		//Sort sort1 = new Sort(Sort.Direction.DESC, "column_name","bus_description");

		if (page == null) {
			page = 0;
		}

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Sort sort1 = new Sort(Sort.Direction.ASC, "id");
		Pageable pageable = new PageRequest(page, per_page, sort1);
		List<TablesColumns> columns = new ArrayList<TablesColumns>();
		String[] parts = sort.split(Pattern.quote("|"));

		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;
			sort1=new Sort(Sort.Direction.DESC, parts[0].trim());
			System.out.println(">>>>> "+parts[0].trim());

		} else {
			sortOrder = SortOrder.ASC;
			sort1=new Sort(Sort.Direction.ASC, parts[0].trim());
			System.out.println(">>>>> "+parts[0].trim());
		}
		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		response = client.prepareSearch("md_attributes_indx".toLowerCase()).setTypes("tablescolumn")
				.setQuery(QueryBuilders.matchAllQuery())
				.addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder)).setSize(pageable.getPageSize())
				.setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute().actionGet();

		for (SearchHit hit : response.getHits()) {
			// allTablesLst.add(hit.getSource());
			TablesColumns column;
			try {

				column = mapper.readValue(hit.getSourceAsString(), TablesColumns.class);
				columns.add(column);

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

		Page<TablesColumns> tableInfo = new PageImpl(columns, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructColumnPagination(tableInfo, "/md_attributes_indx/all");
		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();
		System.out.println("content "+tableInfo.getContent());
		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

		// return return_object;
	}

	private Map<String, Object> constructColumnPagination(Page<TablesColumns> pageT, String url) {
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
		paginationData.put("to", (pageT.getSize()+1) + pageT.getSize() * (pageT.getNumber()));
		paginationData.put("nextpage", pageT.hasNextPage());
		paginationData.put("isFirstPage", pageT.isFirstPage());
		paginationData.put("isLastPage", pageT.isLastPage());
		return paginationData;
	}

//-----------------------------------------------------------------------------------------------------------------------		
	// Method 6: Get records by searching "table_desc","table_name","data_domain"
	// from md_table_indx
	@GetMapping("/md_table_indx/searchall/{searchKey}")
	public List<Map<String, Object>> searchTableRecord(@PathVariable String searchKey) {

		MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery(searchKey, "Table Description", "Table Name",
				"Data Domain");
		builder.analyzer("standard").field("Table Description", 40).field("Table Name", 20).field("Data Domain", 20)
				.cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO).maxExpansions(100).prefixLength(10)
				.type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);

		int scrollSize = 1000;

		List<Map<String, Object>> allTablesLst = new ArrayList<Map<String, Object>>();

		SearchResponse response = null;
		int i = 0;

		while (response == null || response.getHits().hits().length != 0) {
			response = client.prepareSearch("md_table_indx".toLowerCase()).setTypes("tablestable").setQuery(builder)
					.setSize(scrollSize).setFrom(i * scrollSize).execute().actionGet();
			for (SearchHit hit : response.getHits()) {
				allTablesLst.add(hit.getSource());
			}
			i++;
		}
		return allTablesLst;
	}

	// Get record by only searching table name in md_table_indx
	@GetMapping("/md_table_indx/searchtable/{searchKey}")
	public ResponseEntity<Map<String, Object>> searchOnlyTableRecord(@PathVariable String searchKey,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {
		
	//	Sort sort1 = null;
		Sort sort1 = new Sort(Sort.Direction.DESC, "storage_type","source_system","schema","table_desc","row_count","data_as_of_date","load_date");

		if (page == null) {
			page = 0;
		}

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Pageable pageable = new PageRequest(page, per_page, sort1);
		List<TablesInfo> tables = new ArrayList<TablesInfo>();
		String[] parts = sort.split(Pattern.quote("|"));

		SortOrder sortOrder;
		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;

		} else {
			sortOrder = SortOrder.ASC;
		}
		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery(searchKey, "Table Name");

		builder.analyzer("standard").field("Table Name", 20).cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO)
				.maxExpansions(100).prefixLength(10).type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);

		response = client.prepareSearch("md_table_indx".toLowerCase()).setTypes("tablestable").setQuery(builder)
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

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

		// return return_object;
	}

	// Get records by ONLY searching "data_domain" from md_table_indx
	@PostMapping("/md_table_indx/search/dataDomain/{searchKey}")
	public List<Map<String, Object>> searchDataDomainTable(@PathVariable String searchKey) {

		MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery(searchKey, "Data Domain");
		builder.analyzer("standard").field("Data Domain", 20).cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO)
				.maxExpansions(100).prefixLength(10).type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);

		int scrollSize = 1000;

		List<Map<String, Object>> allTablesLst = new ArrayList<Map<String, Object>>();

		SearchResponse response = null;
		int i = 0;

		while (response == null || response.getHits().hits().length != 0) {
			response = client.prepareSearch("md_table_indx".toLowerCase()).setTypes("tablestable").setQuery(builder)
					.setSize(scrollSize).setFrom(i * scrollSize).execute().actionGet();
			for (SearchHit hit : response.getHits()) {
				allTablesLst.add(hit.getSource());
			}
			i++;
		}
		return allTablesLst;
	}

	// Get records by searching {searchKey} + data_domain from md_table_indx
	@GetMapping(value = { "/md_table_indx/filterTableRecords/{searchKey}/{dataDomain}",
			"/md_table_indx/filterTableRecords/{searchKey}" })
	public ResponseEntity<Map<String, Object>> filterTableRecords(@PathVariable Optional<String> searchKey,
			@PathVariable Optional<String> dataDomain,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {
		
		//Sort sort1 = null;
		Sort sort1 = new Sort(Sort.Direction.DESC, "storage_type","source_system","schema","table_desc","row_count","data_as_of_date","load_date");

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

		String[] arrDataDomain = null;
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
			System.out.println("Data domains" + tokens[0]);
			TermsQueryBuilder builder2 = QueryBuilders.termsQuery("Data Domain", tokens);
			boolQueryBuilder.must(builder2);
		}
		
		if (searchKey.isPresent()) {
			
			  boolQueryBuilder.must(QueryBuilders.wildcardQuery("_all", "*" +
			  searchKey.get() + "*"));
		}
		
		
		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
		
		response = client.prepareSearch("md_table_indx".toLowerCase())
				.setTypes("tablestable")
				.setQuery(boolQueryBuilder)
				.addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder))
				.setSize(pageable.getPageSize())
				.setFrom(pageable.getPageNumber() * pageable.getPageSize())
				.execute()
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
		Map<String, Object> paginationData = constructTablePagination(tableInfo, "/md_table_indx/filterTableRecords");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

	}

//Get records by searching {searchKey} + data_domain from md_attributes_indx
	@GetMapping("/md_attributes_indx/filterColumnsRecords/{searchKey}/{dataDomain}")
	public ResponseEntity<String> filterColumnsRecords(@PathVariable Optional<String> searchKey,
			@PathVariable Optional<String> dataDomain,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {

		//Sort sort1 = null;
		Sort sort1 = new Sort(Sort.Direction.DESC, "column_name","bus_description");

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

		String[] arrDataDomain = null;
		List<TablesColumns> tables = new ArrayList<TablesColumns>();
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
			System.out.println("Data domains" + tokens[0]);
			TermsQueryBuilder builder2 = QueryBuilders.termsQuery("Data Domain", tokens);
			boolQueryBuilder.must(builder2);

		}
		if (searchKey.isPresent()) {

			  boolQueryBuilder.must(QueryBuilders.wildcardQuery("Column Name", "*" +
			  searchKey.get() + "*")); 
			  
			  MultiMatchQueryBuilder builder1 = QueryBuilders.multiMatchQuery(searchKey.get(), "Column Name",
			  "Business Description");
			  
			  // String str = searchKey.
			  
			  builder1.analyzer("standard").field("Column Name",
			  20).field("Business Description", 20)
			  .cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO).maxExpansions(100).
			  prefixLength(10) .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);
			  boolQueryBuilder.should(builder1);
			 
//			  boolQueryBuilder.must(QueryBuilders.wildcardQuery("_all", "*" +
//			  searchKey.get() + "*"));

		}

		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		response = client.prepareSearch("md_attributes_indx".toLowerCase()).setTypes("tablescolumn")
				.setQuery(boolQueryBuilder).addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder))
				.setSize(pageable.getPageSize()).setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute()
				.actionGet();

		response.getHits().forEach(hit -> {
			TablesColumns column;

			String source = hit.getSourceAsString();
			try {
				column = mapper.readValue(hit.getSourceAsString(), TablesColumns.class);

				tables.add(column);
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
		Page<TablesColumns> tableInfo = new PageImpl(tables, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructColumnPagination(tableInfo,
				"/md_attributes_indx/filterTableRecords");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

	}

	// Get table details by searching table_name from md_attributes_indx
	@GetMapping("/md_attributes_indx/search/tabledetails/{searchKey}")
	public ResponseEntity<String> getTableDetails(@PathVariable Optional<String> searchKey,
			@PathVariable Optional<String> dataDomain,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {

		//Sort sort1 = null;
		Sort sort1 = new Sort(Sort.Direction.DESC, "column_name","bus_description");

		if (page == null) {
			page = 0;
		}

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Pageable pageable = new PageRequest(page, per_page, sort1);

		String[] arrDataDomain = null;
		List<TablesColumns> tables = new ArrayList<TablesColumns>();
		String[] parts = sort.split(Pattern.quote("|"));
		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;

		} else {
			sortOrder = SortOrder.ASC;
		}
		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery(searchKey.get(), "Table Name");
		builder.analyzer("standard").field("Table Name", 20).operator(Operator.AND).fuzziness(Fuzziness.AUTO)
				.maxExpansions(100).prefixLength(0).type(MultiMatchQueryBuilder.Type.BEST_FIELDS);
		response = client.prepareSearch("md_attributes_indx".toLowerCase()).setTypes("tablescolumn").setQuery(builder)
				.addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder)).setSize(pageable.getPageSize())
				.setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute().actionGet();

		response.getHits().forEach(hit -> {
			TablesColumns column;

			try {
				column = mapper.readValue(hit.getSourceAsString(), TablesColumns.class);

				tables.add(column);
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
		Page<TablesColumns> tableInfo = new PageImpl(tables, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructColumnPagination(tableInfo,
				"/md_attributes_indx/search/tabledetails");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);
	}

	// Get records by searching "column_name","bus_description" from
	// md_attributes_indx
	@PostMapping("/md_attributes_indx/search/{searchKey}")
	public List<Map<String, Object>> searchTableRecordColumns(@PathVariable String searchKey) {

		MultiMatchQueryBuilder builder = QueryBuilders.multiMatchQuery(searchKey, "Column Name",
				"Business Description");
		builder.analyzer("standard").field("Column Name", 20).field("Business Description", 20).operator(Operator.AND)
				.fuzziness(Fuzziness.AUTO).maxExpansions(100).prefixLength(0)
				.type(MultiMatchQueryBuilder.Type.BEST_FIELDS);

		int scrollSize = 1000;

		List<Map<String, Object>> allTablesLst = new ArrayList<Map<String, Object>>();

		SearchResponse response = null;

		int i = 0;

		while (response == null || response.getHits().hits().length != 0) {
			response = client.prepareSearch("md_attributes_indx".toLowerCase()).setTypes("tablescolumn")
					.setQuery(builder).setSize(scrollSize).setFrom(i * scrollSize).execute().actionGet();
			for (SearchHit hit : response.getHits()) {
				allTablesLst.add(hit.getSource());
			}
			i++;
		}
		return allTablesLst;
	}

//	-----------------------------------------------------------------------------------------------------------------------	
	// method 7: Searching distinct data_domain value from md_table_indx
	@GetMapping("/md_table_indx/dataDomain")
	public List<String> getDistinctLabels() throws InterruptedException, ExecutionException {
		SearchRequestBuilder aggregationQuery = client.prepareSearch("md_table_indx").setTypes("tablestable")
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("field").field("Data Domain.keyword").size(100));

		SearchResponse response = aggregationQuery.execute().actionGet();
		Aggregation aggregation = response.getAggregations().get("field");
		StringTerms st = (StringTerms) aggregation;
		return st.getBuckets().stream().map(bucket -> bucket.getKeyAsString()).collect(toList());
	}
	
	
//	-----------------------------------------------------------------------------------------------------------------------	
	// method 8: Get records by only searching data_domain from md_table_indx
	@GetMapping(value = { "/md_table_indx/searchTBDiagramDomain/{searchKey}"})
	public ResponseEntity<Map<String, Object>> searchTBDiagramDomain(
			@PathVariable Optional<String> searchKey,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {
		
		Sort sort1 = null;

		if (page == null) {
			page = 0;
		}

		System.out.println("SORT : " + sort1);

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Pageable pageable = new PageRequest(page, per_page, sort1);

		String[] arrDataDomain = null;
		List<TablesInfo> tables = new ArrayList<TablesInfo>();
		List<Map<String, Object>> allTablesLst = new ArrayList<Map<String, Object>>();

		String[] parts = sort.split(Pattern.quote("|"));
		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;

		} else {
			sortOrder = SortOrder.ASC;
		}
		/*
		 * if (dataDomains != null) { arrDataDomain = dataDomains.split(","); }
		 */

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
//		if (searchKey.isPresent()) {
//		    String[] tokens = searchKey.get().toLowerCase().split(",");
//		    System.out.println("Data domains" + tokens[0]);
//		    TermsQueryBuilder builder2 = QueryBuilders.termsQuery("Data Domain", tokens);
////		    boolQueryBuilder.must(builder2);
//		    boolQueryBuilder.must(builder2);
//		}
		
		
		if (searchKey.isPresent()) {
			MultiMatchQueryBuilder builder2 = QueryBuilders.multiMatchQuery(searchKey.get(), "Data Domain");
			  
			builder2.analyzer("standard").field("Data Domain",20).cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO).maxExpansions(100).
			prefixLength(10) .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);

			boolQueryBuilder.should(builder2);
		}

		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		response = client.prepareSearch("md_table_indx".toLowerCase()).setTypes("tablestable")
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
		Map<String, Object> paginationData = constructTablePagination(tableInfo, "/md_table_indx/searchTBDiagramDomain");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tableInfo.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

	}
	
	// method 9: Get records by only searching data_domain from md_attribute_indx
	@GetMapping("/md_attributes_indx/searchAttDiagramDomain/{searchKey}")
	public ResponseEntity<String> searchAttDiagramDomain(@PathVariable Optional<String> searchKey,
			@RequestParam(name = "page", defaultValue = "1", required = false) Integer page,
			@RequestParam(name = "per_page", defaultValue = "10") Integer per_page,
			@RequestParam(name = "sort", defaultValue = "id|DESC") String sort) {
		
		Sort sort1 = null;

		if (page == null) {
			page = 0;
		}

		System.out.println("SORT : " + sort1);

		if (page >= 1) {
			// Front-end problem forces us to mad solutions
			page--;
		}
		Pageable pageable = new PageRequest(page, per_page, sort1);

		String[] arrDataDomain = null;
		List<TablesColumns> tables = new ArrayList<TablesColumns>();
		String[] parts = sort.split(Pattern.quote("|"));
		SortOrder sortOrder;

		if ("DESC".equalsIgnoreCase(parts[1].trim())) {
			sortOrder = SortOrder.DESC;

		} else {
			sortOrder = SortOrder.ASC;
		}
		/*
		 * if (dataDomains != null) { arrDataDomain = dataDomains.split(","); }
		 */

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

//		if (searchKey.isPresent()) {
//			String[] tokens = searchKey.get().toLowerCase().split(",");
//			System.out.println("Data domains" + tokens[0]);
//			TermsQueryBuilder builder2 = QueryBuilders.termsQuery("Data Domain", tokens);
//			boolQueryBuilder.must(builder2);
//
//		}
		
		if (searchKey.isPresent()) {
			MultiMatchQueryBuilder builder2 = QueryBuilders.multiMatchQuery(searchKey.get(), "Data Domain");
			  
			builder2.analyzer("standard").field("Data Domain",20).cutoffFrequency(0.001f).fuzziness(Fuzziness.AUTO).maxExpansions(100).
			prefixLength(10) .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).boost(20);

			boolQueryBuilder.should(builder2);
		}

		SearchResponse response = null;
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		response = client.prepareSearch("md_attributes_indx".toLowerCase()).setTypes("tablescolumn")
				.setQuery(boolQueryBuilder).addSort(SortBuilders.fieldSort(parts[0].trim()).order(sortOrder))
				.setSize(pageable.getPageSize()).setFrom(pageable.getPageNumber() * pageable.getPageSize()).execute()
				.actionGet();

		response.getHits().forEach(hit -> {
			String source = hit.getSourceAsString();
			try {
				TablesColumns table = mapper.readValue(source, TablesColumns.class);
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
		Page<TablesInfo> tablesColumns = new PageImpl(tables, pageable, response.getHits().getTotalHits());
		Map<String, Object> paginationData = constructTablePagination(tablesColumns, "/md_attributes_indx/searchAttDiagramDomain");

		Map<String, Object> linksData = new HashMap<String, Object>();
		Map<String, Object> mainData = new HashMap<String, Object>();

		linksData.put("data", tablesColumns.getContent());
		linksData.put("pagination", paginationData);
		mainData.put("links", linksData);
		return new ResponseEntity(mainData, HttpStatus.OK);

	}
}
