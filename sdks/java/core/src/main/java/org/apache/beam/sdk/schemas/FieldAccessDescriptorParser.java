/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.DotExpressionContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.FieldSpecifierContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.QualifiedComponentContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.QualifyComponentContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.SimpleIdentifierContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.WildcardContext;

class FieldAccessDescriptorParser {
  static FieldAccessDescriptor parse(String expr) {
    CharStream charStream = CharStreams.fromString(expr);
    org.apache.beam.sdk.schemas.FieldSpecifierNotationLexer lexer =
        new org.apache.beam.sdk.schemas.FieldSpecifierNotationLexer(charStream);
    TokenStream tokens = new CommonTokenStream(lexer);
    org.apache.beam.sdk.schemas.FieldSpecifierNotationParser parser =
        new org.apache.beam.sdk.schemas.FieldSpecifierNotationParser(tokens);
    return new BuildFieldAccessDescriptor().visit(parser.dotExpression());
  }

  public static class BuildFieldAccessDescriptor
      extends org.apache.beam.sdk.schemas.FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {

    @Override
    public FieldAccessDescriptor visitFieldSpecifier(FieldSpecifierContext ctx) {
      return ctx.dotExpression().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitDotExpression(DotExpressionContext ctx) {
      return null;
    }

    @Override
    public FieldAccessDescriptor visitQualifyComponent(QualifyComponentContext ctx) {
      return ctx.qualifiedComponent().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitSimpleIdentifier(SimpleIdentifierContext ctx) {
      return null;
    }

    @Override
    public FieldAccessDescriptor visitWildcard(WildcardContext ctx) {
      return null;
    }

    @Override
    public FieldAccessDescriptor visitQualifiedComponent(QualifiedComponentContext ctx) {
      return null;
    }
  }
}
