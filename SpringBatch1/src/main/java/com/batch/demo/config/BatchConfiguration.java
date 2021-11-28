package com.batch.demo.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.batch.demo.listener.HelloWorldJobExecutionListener;
import com.batch.demo.listener.HelloWorldStepExecutionListener;
import com.batch.demo.model.Product;
import com.batch.demo.processor.InMemItemProcessor;
import com.batch.demo.reader.InMemItemReader;
import com.batch.demo.reader.ProductServiceAdapter;
import com.batch.demo.service.ProductService;
import com.batch.demo.writer.ConsoleItemWriter;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	private HelloWorldJobExecutionListener hwJobExecutionListener;

	@Autowired
	private HelloWorldStepExecutionListener hwStepExecutionListener;

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private ProductService prodService;
	
	@Autowired
	private ProductServiceAdapter prodServAdapter;

	@Bean
	public Step stepOne() {
		return steps.get("step1").listener(hwStepExecutionListener).tasklet(helloWorldTasklet()).build();

	}

	@Bean
	public Step stepTwo() {
		return steps.get("step2").<Product, Product>chunk(3).reader(
				serviceItemReader()).writer(writer())
				//serviceItemReader()).writer(writer())
				//jsonItemReader(null)).writer(writer())
				//jdbcCursorItemReader()).writer(writer())
				/*
				 * staxEventItemReader(null)) .writer(writer())
				 */
				// flatFileItemReader(null)).writer(writer())
				// .processor(processor()).writer(writer())
				.build();
	}

	private ItemWriter<? super Product> writer() {
		return new ConsoleItemWriter();
	}

	private ItemProcessor<? super Integer, ? extends Integer> processor() {
		return new InMemItemProcessor();
	}

	private ItemReader<? extends Integer> reader() {
		return new InMemItemReader();
	}
	
	@StepScope
	@Bean
	public ItemReaderAdapter serviceItemReader() {
		ItemReaderAdapter reader = new ItemReaderAdapter();
		reader.setTargetObject(prodServAdapter);
        //reader.setTargetObject(prodService);
        reader.setTargetMethod("nextProduct");

        return reader;
	}

	@StepScope
	@Bean
	public FlatFileItemReader<Product> flatFileItemReader(
			@Value("#{jobParameters['fileInput']}") FileSystemResource fileResource) {

		FlatFileItemReader<Product> flatFileItemReader = new FlatFileItemReader<>();

		// Locate File
		flatFileItemReader.setResource(fileResource);

		// Line Mapper
		// create the line Mapper
		flatFileItemReader.setLineMapper(new DefaultLineMapper<Product>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "prodId", "productName", "prodDesc", "price", "unit" });
						setDelimiter(",");
					}
				});

				setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
					{
						setTargetType(Product.class);
					}
				});
			}
		}

		);

		// reader to skip the header
		flatFileItemReader.setLinesToSkip(1);

		return flatFileItemReader;

	}

	@SuppressWarnings("unchecked")
	@StepScope
	@Bean
	public FlatFileItemReader flatfixFileItemReader(
			@Value("#{jobParameters['fileInput']}") FileSystemResource inputFile) {
		FlatFileItemReader reader = new FlatFileItemReader();
		// step 1 let reader know where is the file
		reader.setResource(inputFile);

		// create the line Mapper
		reader.setLineMapper(new DefaultLineMapper<Product>() {
			{
				setLineTokenizer(new FixedLengthTokenizer() {
					{
						setNames(new String[] { "prodId", "productName", "productDesc", "price", "unit" });
						setColumns(new Range(1, 16), new Range(17, 41), new Range(42, 65), new Range(66, 73),
								new Range(74, 80)

						);
					}
				});

				setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
					{
						setTargetType(Product.class);
					}
				});
			}
		}

		);
		// step 3 tell reader to skip the header
		reader.setLinesToSkip(1);
		return reader;

	}

	@SuppressWarnings("unchecked")
	@Bean
	public JdbcCursorItemReader jdbcCursorItemReader() {
		JdbcCursorItemReader reader = new JdbcCursorItemReader();
		reader.setDataSource(this.dataSource);
		reader.setSql(
				"select prod_id as productID, prod_name as productName, prod_desc as ProductDesc, unit, price from products");
		reader.setRowMapper(new BeanPropertyRowMapper() {
			{
				setMappedClass(Product.class);
			}
		});
		return reader;
	}

	@StepScope
	@Bean
	public JsonItemReader jsonItemReader(@Value("#{jobParameters['fileInput']}") FileSystemResource inputFile) {
		JsonItemReader reader = new JsonItemReader(inputFile, new JacksonJsonObjectReader(Product.class));
		return reader;

	}

	@SuppressWarnings("rawtypes")
	@StepScope
	@Bean
	public StaxEventItemReader staxEventItemReader(
			@Value("#{jobParameters['fileInput']}") FileSystemResource fileResource) {
		// read xml file
		StaxEventItemReader reader = new StaxEventItemReader();
		reader.setResource(fileResource);
		// let reader know which tag describe the domain object
		reader.setFragmentRootElementName("product");

		// how to parse xml and which domain object to be mapped
		reader.setUnmarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(Product.class);
			}
		});

		return reader;

	}

	private Tasklet helloWorldTasklet() {

		return new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Hello World");
				return RepeatStatus.FINISHED;
			}
		};
	}

	@Bean
	public Job helloWorldJob() {
		return jobs.get("helloWorldJob").incrementer(new RunIdIncrementer()).listener(hwJobExecutionListener)
				.start(stepOne()).next(stepTwo()).build();
	}

}
