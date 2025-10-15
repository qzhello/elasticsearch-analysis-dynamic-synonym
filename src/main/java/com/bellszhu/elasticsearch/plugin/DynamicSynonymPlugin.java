package com.bellszhu.elasticsearch.plugin;

import com.bellszhu.elasticsearch.plugin.synonym.analysis.DynamicSynonymGraphTokenFilterFactory;
import com.bellszhu.elasticsearch.plugin.synonym.analysis.DynamicSynonymTokenFilterFactory;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.plugins.AnalysisPlugin.requiresAnalysisSettings;


/**
 * @author bellszhu
 */
public class DynamicSynonymPlugin extends Plugin implements AnalysisPlugin {

    private Client client;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        client = services.client();

        DynamicSynonymTokenFilterFactory.client = client;
        return Collections.emptyList();
    }


    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("dynamic_synonym", requiresAnalysisSettings((indexSettings, env, name, settings) -> new DynamicSynonymTokenFilterFactory(indexSettings, env, name, settings)));
        extra.put("dynamic_synonym_graph", requiresAnalysisSettings((indexSettings, env, name, settings) -> new DynamicSynonymGraphTokenFilterFactory(indexSettings, env, name, settings)));
        return extra;
    }


}