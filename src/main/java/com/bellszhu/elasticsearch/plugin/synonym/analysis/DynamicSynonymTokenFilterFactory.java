package com.bellszhu.elasticsearch.plugin.synonym.analysis;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author bellszhu
 */
public class DynamicSynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");


    private static final ScheduledExecutorService synonymExpiredSchedule = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("synonym-location-clear-Thread");
        return thread;
    });

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
        return thread;
    });
    private static volatile Map<String, ScheduledFuture<?>> scheduledFutureHashMap = new ConcurrentHashMap<>();

    private final String location;
    private final boolean expand;
    private final boolean lenient;
    private final String format;
    private final int interval;
    protected SynonymMap synonymMap;
    protected Map<AbsSynonymFilter, Integer> dynamicSynonymFilters = new WeakHashMap<>();
    protected final Environment environment;
    protected final AnalysisMode analysisMode;
    public static Client client;


    static {
        synonymExpiredSchedule.scheduleAtFixedRate(() -> {
            try {
                GetSettingsRequest request = new GetSettingsRequest().indices("_all");
                client.admin().indices().getSettings(request, new ActionListener<>() {
                    @Override
                    public void onResponse(GetSettingsResponse response) {
                        Map<String, String> synonymPaths = new HashMap<>();
                        for (Map.Entry<String, Settings> entry : response.getIndexToSettings().entrySet()) {
                            String indexName = entry.getKey();
                            Settings indexSettings = entry.getValue();

                            String filteredSettings = filterSynonymPaths(indexSettings);
                            if (!filteredSettings.isEmpty()) {
                                synonymPaths.put(indexName, filteredSettings);
                            }
                        }
                        // 看哪个path不存在了，移除掉
                        Set<String> synonymsPath = new HashSet<>();
                        for (Map.Entry<String, String> entry : synonymPaths.entrySet()) {
                            synonymsPath.add(entry.getValue());
                        }
                        logger.info("all synonymsPath: {}", synonymsPath);
                        logger.info("current schedule synonymsPath: {}", new ArrayList<>(scheduledFutureHashMap.keySet()));

                        Set<String> removedSynonyms = new HashSet<>();
                        scheduledFutureHashMap.forEach((k, v) -> {
                            if (!synonymsPath.contains(k)) {
                                v.cancel(true);
                                removedSynonyms.add(k);
                            }
                        });
                        for (String removeKey : removedSynonyms) {
                            scheduledFutureHashMap.remove(removeKey);
                            logger.info("expired synonym location: {}", removeKey);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("get _all indices settings failed.", e);
                    }
                });
            } catch (Exception e) {
                logger.error("clear expired synonym location failed", e);
            }

        }, 0, 60, TimeUnit.SECONDS);
    }

    public DynamicSynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) throws IOException {
        super(name, settings);
        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException("dynamic synonym requires `synonyms_path` to be configured");
        }
        if (settings.get("ignore_case") != null) {
        }

        this.interval = settings.getAsInt("interval", 60);
        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
        this.format = settings.get("format", "");
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;
        this.environment = env;

        /**
         * 请求settings，获取索引配置，过滤当前location已经不存在的移除并清理定时任务
         */

    }

    /**
     * 过滤出包含 synonyms_path 的设置项
     */
    private static String filterSynonymPaths(Settings settings) {

        // 遍历所有设置项，查找包含 synonyms_path 的项
        for (String key : settings.keySet()) {
            if (key.contains("synonyms_path")) {
                return settings.get(key);
            }
        }
        return "";
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call getChainAwareTokenFilterFactory to specialize this factory for an analysis chain first");
    }

    public TokenFilterFactory getChainAwareTokenFilterFactory(IndexService.IndexCreationContext context, TokenizerFactory tokenizer, List<CharFilterFactory> charFilters, List<TokenFilterFactory> previousTokenFilters, Function<String, TokenFilterFactory> allFilters) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters);
        synonymMap = buildSynonyms(analyzer);
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // fst is null means no synonyms
                if (synonymMap.fst == null) {
                    return tokenStream;
                }
                DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, false);
                dynamicSynonymFilters.put(dynamicSynonymFilter, 1);

                return dynamicSynonymFilter;
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    Analyzer buildSynonymAnalyzer(TokenizerFactory tokenizer, List<CharFilterFactory> charFilters, List<TokenFilterFactory> tokenFilters) {
        return new CustomAnalyzer(tokenizer, charFilters.toArray(new CharFilterFactory[0]), tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new));
    }

    SynonymMap buildSynonyms(Analyzer analyzer) {
        try {
            return getSynonymFile(analyzer).reloadSynonymMap();
        } catch (Exception e) {
            logger.error("failed to build synonyms", e);
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    SynonymFile getSynonymFile(Analyzer analyzer) {
        try {
            SynonymFile synonymFile;
            if (location.startsWith("http://") || location.startsWith("https://")) {
                synonymFile = new RemoteSynonymFile(environment, analyzer, expand, lenient, format, location);
            } else {
                synonymFile = new LocalSynonymFile(environment, analyzer, expand, lenient, format, location);
            }

            // 判断是否存在location，不存在，则新建立定时任务
            if (!scheduledFutureHashMap.containsKey(location)) {
                scheduledFutureHashMap.putIfAbsent(location, pool.scheduleAtFixedRate(new Monitor(synonymFile), interval, interval, TimeUnit.SECONDS));
            }

            return synonymFile;
        } catch (Exception e) {
            logger.error("failed to get synonyms: " + location, e);
            throw new IllegalArgumentException("failed to get synonyms : " + location, e);
        }
    }

    public class Monitor implements Runnable {

        private SynonymFile synonymFile;

        Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
        }

        @Override
        public void run() {
            try {
                // logger.info("===== Monitor =======");
                if (synonymFile.isNeedReloadSynonymMap()) {
                    synonymMap = synonymFile.reloadSynonymMap();
                    for (AbsSynonymFilter dynamicSynonymFilter : dynamicSynonymFilters.keySet()) {
                        dynamicSynonymFilter.update(synonymMap);
                        logger.debug("success reload synonym");
                    }
                }
            } catch (Exception e) {
                logger.info("Monitor error", e);
//                e.printStackTrace();
                logger.error(e);
            }
        }
    }

}
