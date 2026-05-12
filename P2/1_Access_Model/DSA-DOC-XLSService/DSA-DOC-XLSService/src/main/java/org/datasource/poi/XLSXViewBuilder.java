package org.datasource.poi;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
* Convert the List<Map<String, Object>> model of Sheet-Rows (provided by XLSXTupleViewBuilder)
* into a Java POJO Model List of <V>, where <V> will be the JavaBean class to represent the Row structure
 */
public class XLSXViewBuilder<V> extends XLSXTupleViewBuilder{
	// Data Cache
	private List<V> viewList;
	public List<V> getViewList() {
		return viewList;
	}
	//
	public XLSXViewBuilder(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception {
		super(dataSourceConnector);
	}
	public XLSXViewBuilder(){}
	//
	private ViewListAdapter<V> viewListAdapter;
	public void setViewAdapter(ViewAdapter<V> viewAdapter) {
		this.viewListAdapter = new ViewListAdapter(viewAdapter);
	}

	// Builder Workflow
	public XLSXViewBuilder build() throws Exception{
		if (this.viewListAdapter == null)
			throw new Exception("ViewAdapter not set: call setViewAdapter first!");
		//
		this.viewList = this.viewListAdapter.map(super.build().getTupleList());
		return this;
	}
}
/* Convert List of tuples (as Maps) into a List of JavaBeans <V> Instances */
class ViewListAdapter<V> {
	private ViewAdapter<V> viewAdaptor;

	public ViewListAdapter(ViewAdapter<V> viewAdaptor) {
		this.viewAdaptor = viewAdaptor;
	}

	public List<V> map(List<Map<String, Object>> tupleList){
		List<V> viewList = new ArrayList<>();
		for (Map<String, Object> tuple: tupleList)
			viewList.add(this.viewAdaptor.map(tuple));
		return viewList;
	}
}