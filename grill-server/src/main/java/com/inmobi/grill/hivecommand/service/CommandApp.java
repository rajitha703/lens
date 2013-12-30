package com.inmobi.grill.hivecommand.service;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;


@ApplicationPath("/command")
public class CommandApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
      final Set<Class<?>> classes = new HashSet<Class<?>>();
      // register root resource
      classes.add(CommandResource.class);
      classes.add(MultiPartFeature.class);
      classes.add(LoggingFilter.class);
      return classes;
  }

}
