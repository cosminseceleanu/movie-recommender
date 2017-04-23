package com.movierecommender.main.di;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.movierecommender.main.jobs.*;

public class MainModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<Job> uriBinder = Multibinder.newSetBinder(binder(),  Job.class);
        uriBinder.addBinding().to(StreamingJob.class);
        uriBinder.addBinding().to(ImportRatingsJob.class);
        uriBinder.addBinding().to(ModelFinderJob.class);
        uriBinder.addBinding().to(TrainJob.class);
        uriBinder.addBinding().to(SaveUserRecommendationsJob.class);
        uriBinder.addBinding().to(UserCountJob.class);
    }
}
