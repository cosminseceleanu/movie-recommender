package com.movierecommender.main.di;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.movierecommender.main.jobs.Job;
import com.movierecommender.main.jobs.StreamingJob;
import com.movierecommender.main.jobs.TestStreamingJob;

public class MainModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<Job> uriBinder = Multibinder.newSetBinder(binder(),  Job.class);
        uriBinder.addBinding().to(StreamingJob.class);
        uriBinder.addBinding().to(TestStreamingJob.class);
    }
}
