const uuidv4 = require(`uuid/v4`)
const { buildSchema, printSchema } = require(`gatsby/graphql`)
const {
  wrapSchema,
  introspectSchema,
  RenameTypes,
} = require(`@graphql-tools/wrap`)
const { linkToExecutor } = require(`@graphql-tools/links`)
const { createHttpLink } = require(`apollo-link-http`)
const invariant = require(`invariant`)
const { fetchWrapper } = require(`./fetch`)
const { createDataloaderLink } = require(`./batching/dataloader-link`)

const {
  NamespaceUnderFieldTransform,
  StripNonQueryTransform,
} = require(`./transforms`)

exports.sourceNodes = async (
  { actions, createNodeId, cache, createContentDigest },
  options
) => {
  const { addThirdPartySchema, createNode } = actions
  const {
    url,
    typeName,
    fieldName,
    moduleName,
    moduleSource,
    headers = {},
    fetch = fetchWrapper,
    fetchOptions = {},
    createLink,
    createSchema,
    refetchInterval,
    batch = false,
    transformSchema,
  } = options

  invariant(
    moduleSource && moduleSource.length > 0,
    `\`@lnfusion/gatsby-source-graphql\` requires option \`moduleSource\` to be specified`
  )
  invariant(
    moduleName && moduleName.length > 0,
    `\`@lnfusion/gatsby-source-graphql\` requires option \`moduleName\` to be specified`
  )
  invariant(
    typeName && typeName.length > 0,
    `${moduleName} requires option \`typeName\` to be specified`
  )
  invariant(
    fieldName && fieldName.length > 0,
    `${moduleName} requires option \`fieldName\` to be specified`
  )
  invariant(
    (url && url.length > 0) || createLink,
    `${moduleName} requires either option \`url\` or \`createLink\` callback`
  )

  let link
  if (createLink) {
    link = await createLink(options)
  } else {
    const options = {
      uri: url,
      fetch,
      fetchOptions,
      headers: typeof headers === `function` ? await headers() : headers,
    }
    link = batch ? createDataloaderLink(options) : createHttpLink(options)
  }

  let introspectionSchema

  if (createSchema) {
    introspectionSchema = await createSchema(options)
  } else {
    const cacheKey = `${moduleName}-schema-${typeName}-${fieldName}`
    let sdl = await cache.get(cacheKey)

    if (!sdl) {
      introspectionSchema = await introspectSchema(linkToExecutor(link))
      sdl = printSchema(introspectionSchema)
    } else {
      introspectionSchema = buildSchema(sdl)
    }

    await cache.set(cacheKey, sdl)
  }

  const nodeId = createNodeId(`${moduleName}-${typeName}`)
  const node = createSchemaNode({
    id: nodeId,
    typeName,
    fieldName,
    createContentDigest,
    moduleSource,
  })
  createNode(node)

  const resolver = (parent, args, context) => {
    context.nodeModel.createPageDependency({
      path: context.path,
      nodeId: nodeId,
    })
    return {}
  }

  const defaultTransforms = [
    new StripNonQueryTransform(),
    new RenameTypes(name => `${typeName}_${name}`),
    new NamespaceUnderFieldTransform({
      typeName,
      fieldName,
      resolver,
    }),
  ]

  const schema = transformSchema
    ? transformSchema({
        schema: introspectionSchema,
        link,
        resolver,
        defaultTransforms,
        options,
      })
    : wrapSchema({
        schema: introspectionSchema,
        executor: linkToExecutor(link),
        transforms: defaultTransforms,
      })

  addThirdPartySchema({ schema })

  if (process.env.NODE_ENV !== `production`) {
    if (refetchInterval) {
      const msRefetchInterval = refetchInterval * 1000
      const refetcher = () => {
        createNode(
          createSchemaNode({
            id: nodeId,
            typeName,
            fieldName,
            createContentDigest,
            moduleSource,
          })
        )
        setTimeout(refetcher, msRefetchInterval)
      }
      setTimeout(refetcher, msRefetchInterval)
    }
  }
}

function createSchemaNode({ id, typeName, fieldName, createContentDigest, moduleSource }) {
  const nodeContent = uuidv4()
  const nodeContentDigest = createContentDigest(nodeContent)
  return {
    id,
    typeName: typeName,
    fieldName: fieldName,
    parent: null,
    children: [],
    internal: {
      type: moduleSource,
      contentDigest: nodeContentDigest,
      ignoreType: true,
    },
  }
}
