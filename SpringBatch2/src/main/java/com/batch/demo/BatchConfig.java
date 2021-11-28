package com.batch.demo;

import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.batch.demo.model.Product;
import com.batch.demo.processor.ProductProcessor;
import com.batch.demo.reader.RangePartitioner;
import com.batch.demo.tasklet.BizTasklet3;
import com.batch.demo.tasklet.BizTasklet4;
import com.batch.demo.tasklet.CleanupTasklet;
import com.batch.demo.tasklet.ConsoleTasklet;
import com.batch.demo.tasklet.DownloadTasklet;
import com.batch.demo.tasklet.FileProcessTasklet;
import com.batch.demo.tasklet.PagerDutyTasklet;
import com.batch.demo.writer.ConsoleItemWriter;

@EnableBatchProcessing
@Configuration
public class BatchConfig {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	private DataSource dataSource;

	// @Autowired
	// private ProductServiceAdapter prodServAdapter;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	@StepScope
	public FlatFileItemReader reader(@Value("#{jobParameters[fileInput]}") FileSystemResource inputFile) {

		FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
		flatFileItemReader.setResource(inputFile);
		flatFileItemReader.setLinesToSkip(1);
		flatFileItemReader.setLineMapper(new DefaultLineMapper() {
			{
				setFieldSetMapper(new BeanWrapperFieldSetMapper() {
					{
						setTargetType(Product.class);
					}
				});

				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "productId", "prodName", "productDesc", "price", "unit" });
						setDelimiter(DELIMITER_COMMA);
					}
				});
			}
		});
		return flatFileItemReader;

	}

	@SuppressWarnings("rawtypes")
	@Bean
	@StepScope
	public StaxEventItemWriter xmlWriter(@Value("#{jobParameters[fileOutput]}") FileSystemResource inputFile) {

		XStreamMarshaller marshaller = new XStreamMarshaller();
		StaxEventItemWriter writer = new StaxEventItemWriter();
		HashMap<String, Class> aliases = new HashMap<>();
		aliases.put("product", Product.class);
		marshaller.setAliases(aliases);
		marshaller.setAutodetectAnnotations(true);
		writer.setMarshaller(marshaller);
		writer.setResource(inputFile);
		writer.setRootTagName("Products");

		return writer;

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	@StepScope
	public FlatFileItemWriter writer(@Value("#{jobParameters[fileOutput]}") FileSystemResource inputFile) {
		/*
		 * FlatFileItemWriter flatFileItemWriter = new FlatFileItemWriter<Product>() {
		 * 
		 * @Override public String doWrite(List<? extends Product> items) { for (Product
		 * p : items) { if (p.getProductId() == 9) { throw new
		 * RuntimeException("exception during writing product id-9"); } } return
		 * super.doWrite(items); } };
		 */
		FlatFileItemWriter flatFileItemWriter = new FlatFileItemWriter();
		flatFileItemWriter.setResource(inputFile);
		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator() {
			{
				setDelimiter("|");
				setFieldExtractor(new BeanWrapperFieldExtractor() {
					{
						setNames(new String[] { "productId", "prodName", "productDesc", "price", "unit" });
					}
				});
			}
		});

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {

			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("productId, prodName, productDesc, price, unit");

			}
		});

		flatFileItemWriter.setAppendAllowed(true);
		/*
		 * flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
		 * 
		 * @Override public void writeFooter(Writer writer) throws IOException {
		 * writer.write("The file is created at:- " + new SimpleDateFormat().format(new
		 * Date()));
		 * 
		 * } });
		 */

		return flatFileItemWriter;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	@StepScope
	public JdbcBatchItemWriter dbWriter() {

		JdbcBatchItemWriter jdbcBatchItemWriter = new JdbcBatchItemWriter();
		jdbcBatchItemWriter.setDataSource(dataSource);
		jdbcBatchItemWriter.setSql(
				"insert into products(prod_id, prod_name, prod_desc, unit, price) " + " values (?, ?, ?, ?, ?)");
		jdbcBatchItemWriter.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Product>() {

			@Override
			public void setValues(Product item, PreparedStatement ps) throws SQLException {
				ps.setInt(1, item.getProductId());
				ps.setString(2, item.getProdName());
				ps.setString(3, item.getProductDesc());
				ps.setInt(4, item.getUnit());
				ps.setBigDecimal(5, item.getPrice());

			}
		});

		return jdbcBatchItemWriter;

	}

	@SuppressWarnings("rawtypes")
	@Bean
	@StepScope
	public JdbcBatchItemWriter advDbWriter() {
		return new JdbcBatchItemWriterBuilder<Product>().dataSource(dataSource)
				.sql("insert into products(prod_id, prod_name, prod_desc, unit, price) "
						+ " values (:productId, :prodName, :productDesc, :unit, :price)")
				.beanMapped().build();
	}

	@SuppressWarnings("unchecked")
	@Bean
	public Step stepOne() {
		return steps.get("step1").<Product, Product>chunk(5).reader(reader(null))
				// .writer(advDbWriter()).build();
				// .writer(dbWriter()).build();
				// .writer(xmlWriter(null)).build();
				// .reader(serviceAdapter())
				.processor(new ProductProcessor()).writer(writer(null)).faultTolerant()
				// .retry(ResourceAccessException.class).retryLimit(5)
				// .skip(ResourceAccessException.class).skipLimit(30)
				// .skip(RuntimeException.class)
				// .skipLimit(10)
				// .skipPolicy(new AlwaysSkipItemSkipPolicy())
				// .listener(new ProductSkipListener())
				.build();
	}

	@SuppressWarnings("unchecked")
	@Bean
	public Step multi_thread_step() {

		/*
		 * ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		 * taskExecutor.setCorePoolSize(4); taskExecutor.setMaxPoolSize(4);
		 * taskExecutor.afterPropertiesSet();
		 */

		return steps.get("multi_thread_step").<Product, Product>chunk(5).reader(reader(null))
				.processor(asyncItemProcessor()).writer(asyncItemWriter())
				// .processor(new ProductProcessor())
				// .writer(writer(null))
				// .taskExecutor(taskExecutor)
				.build();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public AsyncItemProcessor asyncItemProcessor() {
		AsyncItemProcessor processor = new AsyncItemProcessor();
		processor.setDelegate(new ProductProcessor());
		processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
		return processor;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	public AsyncItemWriter asyncItemWriter() {
		AsyncItemWriter writer = new AsyncItemWriter();
		writer.setDelegate(writer(null));
		return writer;
	}

	/*
	 * public ItemReaderAdapter<Product> serviceAdapter() {
	 * ItemReaderAdapter<Product> itemReaderAdapter = new ItemReaderAdapter<>();
	 * itemReaderAdapter.setTargetMethod("nextProduct");
	 * itemReaderAdapter.setTargetObject(prodServAdapter); return itemReaderAdapter;
	 * }
	 */

	public Step downloadStep() {
		return steps.get("downloadStep").tasklet(new DownloadTasklet()).build();
	}

	public Step fileProcessStep() {
		return steps.get("fileProcessStep").tasklet(new FileProcessTasklet()).build();
	}

	public Step bizStep3() {
		return steps.get("bizStep3").tasklet(new BizTasklet3()).build();
	}

	public Step bizStep4() {
		return steps.get("bizStep4").tasklet(new BizTasklet4()).build();
	}

	public Step cleanupStep() {
		return steps.get("cleanupStep").tasklet(new CleanupTasklet()).build();
	}
	
	 public Flow splitFlow(){
	      return new FlowBuilder<SimpleFlow>("splitFlow")
	              .split(new SimpleAsyncTaskExecutor())
	              .add(fileFlow(),bizFlow1(),bizFlow2())
	              .build();


	    }

	    public Flow fileFlow(){
	        return new FlowBuilder< SimpleFlow >("fileFlow")
	                .start(downloadStep())
	                .next(fileProcessStep())
	                .build();
	    }

	    public Flow bizFlow1(){
	        return new FlowBuilder< SimpleFlow >("bizFlow1")
	                .start(bizStep3())
	                .build();
	    }

	    public Flow bizFlow2(){
	        return new FlowBuilder< SimpleFlow >("bizFlow2")
	                .start(bizStep4())
	                .from(bizStep4())
	                .on("*")
	                .end()
	                .on("FAILED")
	                .to(pagerDutyStep())
	                .build();
	    }
	    
	    public Step pagerDutyStep() {
			return steps.get("pager-duty-step").tasklet(new PagerDutyTasklet()).build();
	    	
	    }

		/*
		 * @Bean public Job job1(){ return jobs.get("job1") .incrementer(new
		 * RunIdIncrementer()) .start(splitFlow()) .next(cleanupStep()) .end() .build();
		 * }
		 */
	@Bean
	public Job job() {
		return jobs.get("file-writing-job")
				.incrementer(new RunIdIncrementer())
				.start(partitionSetp())
				// .start(step0())
				// .next(stepOne())
				// .next(multi_thread_step())
				.build();
	}

	@Bean
	public Step step0() {
		return steps.get("step0").tasklet(new ConsoleTasklet()).build();

	}
	
	 public Step partitionSetp(){

	        return steps.get("partitionStep")
	                .partitioner(slaveStep().getName(), new RangePartitioner())
	                .step(slaveStep())
	                .gridSize(3)
	                .taskExecutor(new SimpleAsyncTaskExecutor())
	                .build();
	    }


	    @SuppressWarnings("unchecked")
		public Step slaveStep(){
	        return steps.get("slaveStep")
	                .<Product,Product>chunk(5)
	                .reader(pagingItemReader(null,null))
	                .writer(new ConsoleItemWriter())
	                .build();
	    }
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
    @StepScope
	public JdbcPagingItemReader pagingItemReader(@Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
		
		System.out.println("From " + minValue + "to "+ maxValue );
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("prod_id", Order.ASCENDING);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("prod_id, prod_name, prod_desc, unit, price");
        queryProvider.setFromClause("from products");
        queryProvider.setWhereClause("where prod_id >=" + minValue + " and prod_id <" + maxValue);
        queryProvider.setSortKeys(sortKey);

        JdbcPagingItemReader reader = new JdbcPagingItemReader();
        reader.setDataSource(this.dataSource);
        reader.setQueryProvider(queryProvider);
        reader.setFetchSize(1000);

        reader.setRowMapper(new BeanPropertyRowMapper(){
            {
                setMappedClass(Product.class);
            }
        });

        return reader;
		
	}

}
