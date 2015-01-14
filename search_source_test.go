package elastic

import (
	"encoding/json"
	"testing"
)

func TestSearchSourceMatchAllQuery(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceNoFields(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).NoFields()
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":[],"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceFields(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).Fields("message", "tags")
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":["message","tags"],"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceFetchSourceDisabled(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).FetchSource(false)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"_source":false,"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceFetchSourceByWildcards(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	fsc := NewFetchSourceContext(true).Include("obj1.*", "obj2.*").Exclude("*.description")
	builder := NewSearchSource().Query(matchAllQ).FetchSourceContext(fsc)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"_source":{"excludes":["*.description"],"includes":["obj1.*","obj2.*"]},"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceFieldDataFields(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).FieldDataFields("test1", "test2")
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fielddata_fields":["test1","test2"],"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceScriptFields(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	sf1 := NewScriptField("test1", "doc['my_field_name'].value * 2", "", nil)
	sf2 := NewScriptField("test2", "doc['my_field_name'].value * factor", "", map[string]interface{}{"factor": 3.1415927})
	builder := NewSearchSource().Query(matchAllQ).ScriptFields(sf1, sf2)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"query":{"match_all":{}},"script_fields":{"test1":{"script":"doc['my_field_name'].value * 2"},"test2":{"params":{"factor":3.1415927},"script":"doc['my_field_name'].value * factor"}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourcePostFilter(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	pf := NewTermFilter("tag", "important")
	builder := NewSearchSource().Query(matchAllQ).PostFilter(pf)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"post_filter":{"term":{"tag":"important"}},"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceHighlight(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	hl := NewHighlight().Field("content")
	builder := NewSearchSource().Query(matchAllQ).Highlight(hl)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"highlight":{"fields":{"content":{}}},"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceRescoring(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	rescorerQuery := NewMatchQuery("field1", "the quick brown fox").Type("phrase").Slop(2)
	rescorer := NewQueryRescorer(rescorerQuery)
	rescorer = rescorer.QueryWeight(0.7)
	rescorer = rescorer.RescoreQueryWeight(1.2)
	rescore := NewRescore().WindowSize(50).Rescorer(rescorer)
	builder := NewSearchSource().Query(matchAllQ).AddRescore(rescore)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"query":{"match_all":{}},"rescore":{"query":{"query_weight":0.7,"rescore_query":{"match":{"field1":{"query":"the quick brown fox","slop":2,"type":"phrase"}}},"rescore_query_weight":1.2},"window_size":50}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceIndexBoost(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).IndexBoost("index1", 1.4).IndexBoost("index2", 1.3)
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"indices_boost":{"index1":1.4,"index2":1.3},"query":{"match_all":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchSourceMixDifferentSorters(t *testing.T) {
	matchAllQ := NewMatchAllQuery()
	builder := NewSearchSource().Query(matchAllQ).
		Sort("a", false).
		SortWithInfo(SortInfo{Field: "b", Ascending: true}).
		SortBy(NewScriptSort("doc['field_name'].value * factor", "number").Param("factor", 1.1))
	data, err := json.Marshal(builder.Source())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"query":{"match_all":{}},"sort":[{"a":{"order":"desc"}},{"b":{"order":"asc"}},{"_script":{"params":{"factor":1.1},"script":"doc['field_name'].value * factor","type":"number"}}]}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
