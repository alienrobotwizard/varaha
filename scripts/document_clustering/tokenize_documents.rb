#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

require 'rubygems'
require 'wukong'
require 'wukong/encoding'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)
require 'set'

Settings.define :id_field,   :type => Integer, :default => 0, :required => true, :description => "What field to use as the document id. (-1) to assign ids"
Settings.define :text_field, :type => Integer, :default => 1, :required => true, :description => "Which field is the text field?"
Settings.resolve!

STOPWORDS = %w[
a about above across after again against all almost alone along already also
although always among an and another any anybody anyone anything anywhere apos
are area areas around as ask asked asking asks at away

back backed backing backs be became because become becomes been before began
behind being beings best better between big both but by

came can cannot case cases certain certainly clear clearly come could

did differ different differently do does done down down downed downing downs
during

each early either end ended ending ends enough even evenly ever every everybody
everyone everything everywhere

face faces fact facts far felt few find finds first for four from full fully
further furthered furthering furthers

gave general generally get gets give given gives go going good goods got great
greater greatest group grouped grouping groups

had has have having he her here herself high high high higher highest him
himself his how however i if important in interest interested interesting
interests into is it its it's itself

just

keep keeps kind knew know known knows

large largely last later latest least less let lets like likely long longer
longest

made make making man many may me member members men might more most mostly mr
mrs much must my myself

nbsp necessary need needed needing needs never new new newer newest next no
nobody non noone not nothing now nowhere number numbers

of off often old older oldest on once one only open opened opening opens or
order ordered ordering orders other others our out over

part parted parting parts per perhaps place places point pointed pointing points
possible present presented presenting presents problem problems put puts

quite quot

rather really right right room rooms

said same saw say says second seconds see seem seemed seeming seems sees several
shall she should show showed showing shows side sides since small smaller
smallest so some somebody someone something somewhere state states still still
such sure

take taken than that the their them then there therefore these they thing things
think thinks this those though thought thoughts three through thus to today
together too took toward turn turned turning turns two

under until up upon us use used uses

very

want wanted wanting wants was way ways we well wells went were what when where
whether which while who whole whose why will with within without work worked
working works would

year years yet you young younger youngest your yours
].to_set

class GeneralTextTokenizer < Wukong::Streamer::RecordStreamer
  def tokenize text
    return [] if text.blank?
    text = text.gsub(%r{[^[:alpha:]\w\']+}, " ")
    text.gsub!(%r{([[:alpha:]\w])\'([st])},   '\1!\2')
    text.gsub!(%r{[\s\']},         " ")
    text.gsub!(%r{!},              "'")
    # words = text.strip.wukong_encode.split(/\s+/)
    words = text.strip.split(/\s+/)
    words.reject!{|w| w.blank? || (w.length < 3) }
    words
  end

  def tokenize_text_chunk text_chunk
    return [] if text_chunk.blank?
    text_chunk = text_chunk.wukong_decode.downcase
    tokenize(text_chunk.strip)
  end

  def process *args
    tokenize_text_chunk(args[Settings.text_field]).each do |token|
      yield [document_id(args), token] unless STOPWORDS.include?(token)
    end
  end

  def document_id fields
    fields[Settings.id_field] unless Settings.id_field == -1
  end

end

Wukong::Script.new(GeneralTextTokenizer, nil).run
