package hello;

import org.springframework.batch.core.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    /**
     *
     * Step -> ItemReader< <<- FlatFileItemReader<Person>
     *      -> ItemProcessor<Person,Person> <<- PersonItemProcessor
     *      -> ItemWriter<Person> <<- JdbcBatchItemWriter<Person>
     */

    //tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>(){{
                    setTargetType(Person.class);
                }}).build();

    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }
    //end::readerwriterprocessor[]

    //tag::jobstep[]

    /**
     * <p>
     * Job 만드는 법. JobBuilder를 통해 이루어진다.
     * </p>
     * jobBuilder.flow({@link Step} step)메소드를 이용해 Job을 구성하는 step pipeline을 만들 수 있으며,
     * spring-batch의 {@link JobExecutionListener}를 구현한 리스너를 통해
     * Job의 실행 상태와 관련된 callback(옵저버)도 job 빌드과정에서 설정할 수 있다.
     * Job execution 상태를 DB에 유지관리하기 위해 incrementer를 쓸 수도 있다.
     *
     * @param listener
     * @param step1
     * @return
     */
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer()) //DB에 job execution state를 유지,관리하기 위해 필요
                .listener(listener)
                .flow(step1) // 각 step들을 jobBuilder.flow(Step step)으로 job에 등록,배열한다.
                .end()
                .build();
    }

    /**
     * <p>
     * Step 만드는 법. StepBuilder를 통해 이루어진다.
     * </p>
     * <p>
     * Step은 ETL의 3단계 오퍼레이션(Extract, Transformation, Load)을 그대로 추상화하여,
     * Item에 대한 하나의 read, process, write 오퍼레이션을 구성한다.
     *
     * </p>
     * <p>
     * 이는 독립적인 책임을 가진 객체로 분할되어,
     * reader(ItemReader reader), processor(ItemProcessor processor), writer(ItemWriter writer)
     * 메소드로 Step에 연관된다.
     * </p>
     * <p>
     * 커밋의 단위는 chuck(int chunkSize)로 설정한다.
     * </p>
     * @param writer
     * @return
     */
    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(10) // the chunk size (commit interval)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
    //end:jobstep[]




}
